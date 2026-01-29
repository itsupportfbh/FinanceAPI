using Dapper;
using FinanceApi.Data;
using FinanceApi.Repositories;
using Microsoft.Data.SqlClient;
using System.Data;

public class BatchProductionRepository : DynamicRepository, IBatchProductionRepository
{
    public BatchProductionRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory) { }

    public async Task<IEnumerable<BatchProductionHeaderDto>> ListAsync(int top = 200)
    {
        const string sql = @"
SELECT TOP (@Top)
  Id, ProductionPlanId, WarehouseId, BatchNo, Status,
  CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, PostedBy, PostedDate
FROM dbo.BatchProduction
ORDER BY Id DESC;";

        return await Connection.QueryAsync<BatchProductionHeaderDto>(sql, new { Top = top });
    }

    public async Task<BatchProductionGetByIdDto> GetByIdAsync(int id)
    {
        using var conn = (SqlConnection)Connection;
        if (conn.State != ConnectionState.Open) await conn.OpenAsync();

        const string sql = @"
SELECT TOP 1
  Id, ProductionPlanId, WarehouseId, BatchNo, Status,
  CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, PostedBy, PostedDate
FROM dbo.BatchProduction
WHERE Id = @Id;

SELECT
  l.Id, l.BatchProductionId, l.RecipeId, l.FinishedItemId,
  CAST(l.PlannedQty AS DECIMAL(18,4)) AS PlannedQty,
  CAST(l.ActualQty  AS DECIMAL(18,4)) AS ActualQty,
  im.Name AS FinishedItemName
FROM dbo.BatchProductionLines l
LEFT JOIN dbo.ItemMaster im ON im.Id = l.FinishedItemId
WHERE l.BatchProductionId = @Id
ORDER BY l.Id;";

        using var multi = await conn.QueryMultipleAsync(sql, new { Id = id });

        var header = await multi.ReadFirstOrDefaultAsync<BatchProductionHeaderDto>();
        if (header == null) throw new Exception("Batch not found");

        var lines = (await multi.ReadAsync<BatchProductionLineDto>()).AsList();

        return new BatchProductionGetByIdDto { Header = header, Lines = lines };
    }

    public async Task<int> DeleteAsync(int id)
    {
        using var conn = (SqlConnection)Connection;
        if (conn.State != ConnectionState.Open) await conn.OpenAsync();

        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);

        try
        {
            const string lockSql = @"
SELECT Status
FROM dbo.BatchProduction WITH (UPDLOCK, HOLDLOCK)
WHERE Id = @Id;";

            var status = await conn.ExecuteScalarAsync<string>(lockSql, new { Id = id }, tx);
            if (status == null) throw new Exception("Batch not found");
            if (string.Equals(status, "Posted", StringComparison.OrdinalIgnoreCase))
                throw new Exception("Cannot delete. Batch already posted.");

            await conn.ExecuteAsync("DELETE FROM dbo.BatchProductionLines WHERE BatchProductionId=@Id;", new { Id = id }, tx);
            await conn.ExecuteAsync("DELETE FROM dbo.BatchProduction WHERE Id=@Id;", new { Id = id }, tx);

            tx.Commit();
            return id;
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }

    // ✅ ONE METHOD: Save + Post + Reduce ingredients
    public async Task<int> PostAndSaveAsync(BatchProductionSaveRequest req)
    {
        using var conn = (SqlConnection)Connection;
        if (conn.State != ConnectionState.Open) await conn.OpenAsync();

        using var tx = conn.BeginTransaction(IsolationLevel.ReadCommitted);

        try
        {
            // 0) ✅ WarehouseId from ProductionPlan (NOT from UI)
            const string whSql = @"
SELECT WarehouseId
FROM dbo.ProductionPlan
WHERE Id = @PlanId;";
            var warehouseId = await conn.ExecuteScalarAsync<int?>(whSql, new { PlanId = req.ProductionPlanId }, tx);

            if (!warehouseId.HasValue || warehouseId.Value <= 0)
                throw new Exception("WarehouseId not found in ProductionPlan");

            // 1) Create or Update batch header + lines
            int batchId;

            if (!req.Id.HasValue || req.Id.Value <= 0)
            {
                // create
                const string insertHeader = @"
INSERT INTO dbo.BatchProduction
(ProductionPlanId, WarehouseId, BatchNo, Status, CreatedBy, CreatedDate)
VALUES
(@ProductionPlanId, @WarehouseId, @BatchNo, 'Draft', @User, GETDATE());
SELECT CAST(SCOPE_IDENTITY() AS INT);";

                batchId = await conn.ExecuteScalarAsync<int>(insertHeader, new
                {
                    req.ProductionPlanId,
                    WarehouseId = warehouseId.Value,
                    req.BatchNo,
                    req.User
                }, tx);
            }
            else
            {
                batchId = req.Id.Value;

                // prevent repost
                const string lockBatch = @"
SELECT Status
FROM dbo.BatchProduction WITH (UPDLOCK, HOLDLOCK)
WHERE Id=@Id;";
                var st = await conn.ExecuteScalarAsync<string>(lockBatch, new { Id = batchId }, tx);

                if (st == null) throw new Exception("Batch not found");
                if (string.Equals(st, "Posted", StringComparison.OrdinalIgnoreCase))
                    throw new Exception("Already posted");

                const string updateHeader = @"
UPDATE dbo.BatchProduction
SET ProductionPlanId=@ProductionPlanId,
    WarehouseId=@WarehouseId,
    BatchNo=@BatchNo,
    Status='Draft',
    UpdatedBy=@User,
    UpdatedDate=GETDATE()
WHERE Id=@Id;";

                await conn.ExecuteAsync(updateHeader, new
                {
                    Id = batchId,
                    req.ProductionPlanId,
                    WarehouseId = warehouseId.Value,
                    req.BatchNo,
                    req.User
                }, tx);

                await conn.ExecuteAsync("DELETE FROM dbo.BatchProductionLines WHERE BatchProductionId=@Id;", new { Id = batchId }, tx);
            }

            // insert lines
            const string insertLines = @"
INSERT INTO dbo.BatchProductionLines
(BatchProductionId, RecipeId, FinishedItemId, PlannedQty, ActualQty, CreatedDate)
VALUES
(@BatchProductionId, @RecipeId, @FinishedItemId, @PlannedQty, @ActualQty, GETDATE());";

            foreach (var l in req.Lines)
            {
                await conn.ExecuteAsync(insertLines, new
                {
                    BatchProductionId = batchId,
                    RecipeId = l.RecipeId,
                    FinishedItemId = l.FinishedItemId,
                    PlannedQty = l.PlannedQty,
                    ActualQty = l.ActualQty
                }, tx);
            }

            // 2) calculate needs
            const string needSql = @"
;WITH bl AS (
  SELECT RecipeId, CAST(ActualQty AS DECIMAL(18,4)) AS ActualQty
  FROM dbo.BatchProductionLines
  WHERE BatchProductionId = @BatchId
),
rb AS (
  SELECT Id AS RecipeId,
         CAST(NULLIF(ExpectedOutput,0) AS DECIMAL(18,4)) AS BaseOutput
  FROM dbo.RecipeHeader
),
need AS (
  SELECT
    ri.IngredientItemId AS ItemId,
    SUM(
      CAST(ri.Qty AS DECIMAL(18,4)) * (bl.ActualQty / ISNULL(rb.BaseOutput,1))
    ) AS RequiredQty
  FROM bl
  INNER JOIN dbo.RecipeIngredient ri ON ri.RecipeId = bl.RecipeId
  LEFT JOIN rb ON rb.RecipeId = bl.RecipeId
  GROUP BY ri.IngredientItemId
)
SELECT ItemId, CAST(RequiredQty AS DECIMAL(18,4)) AS RequiredQty
FROM need;";

            var needs = (await conn.QueryAsync<(int ItemId, decimal RequiredQty)>(needSql, new { BatchId = batchId }, tx)).ToList();
            if (needs.Count == 0) throw new Exception("No ingredients found for recipes");

            // 3) validate + reduce stock
            const string checkSql = @"
SELECT ISNULL(OnHand,0)
FROM dbo.ItemWarehouseStock
WHERE ItemId=@ItemId AND WarehouseId=@WarehouseId;";

            const string reduceWsSql = @"
UPDATE dbo.ItemWarehouseStock
SET OnHand = CAST(OnHand AS DECIMAL(18,4)) - @Req,
    Available = CAST(Available AS DECIMAL(18,4)) - @Req
WHERE ItemId=@ItemId AND WarehouseId=@WarehouseId;";

            const string reduceIpSql = @"
UPDATE dbo.ItemPrice
SET Qty = CAST(Qty AS DECIMAL(18,4)) - @Req
WHERE ItemId=@ItemId AND WarehouseId=@WarehouseId;";

            foreach (var n in needs)
            {
                var onhand = await conn.ExecuteScalarAsync<decimal>(checkSql,
                    new { n.ItemId, WarehouseId = warehouseId.Value }, tx);

                if (onhand < n.RequiredQty)
                    throw new Exception($"Insufficient stock for ItemId {n.ItemId}. Need {n.RequiredQty} but OnHand {onhand}");

                var wsAff = await conn.ExecuteAsync(reduceWsSql,
                    new { n.ItemId, Req = n.RequiredQty, WarehouseId = warehouseId.Value }, tx);

                if (wsAff == 0)
                    throw new Exception($"ItemWarehouseStock missing for ItemId {n.ItemId} WarehouseId {warehouseId.Value}");

                var ipAff = await conn.ExecuteAsync(reduceIpSql,
                    new { n.ItemId, Req = n.RequiredQty, WarehouseId = warehouseId.Value }, tx);

                if (ipAff == 0)
                    throw new Exception($"ItemPrice missing for ItemId {n.ItemId} WarehouseId {warehouseId.Value}");
            }

            // 4) mark posted
            const string markPosted = @"
UPDATE dbo.BatchProduction
SET Status='Posted',
    PostedBy=@User,
    PostedDate=GETDATE(),
    UpdatedBy=@User,
    UpdatedDate=GETDATE()
WHERE Id=@Id;";

            await conn.ExecuteAsync(markPosted, new { Id = batchId, req.User }, tx);

            tx.Commit();
            return batchId;
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }
}
