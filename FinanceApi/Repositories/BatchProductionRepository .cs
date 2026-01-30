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
  bp.Id,
  bp.ProductionPlanId,
  bp.WarehouseId,
  bp.BatchNo,
  bp.Status,
  bp.CreatedBy,
  bp.CreatedDate,
  bp.UpdatedBy,
  bp.UpdatedDate,
  bp.PostedBy,
  bp.PostedDate,

  -- ✅ from ProductionPlan
  p.ProductionPlanNo,
  CAST(p.PlanDate AS DATE) AS PlanDate,
  l.name

FROM dbo.BatchProduction bp
LEFT JOIN dbo.ProductionPlan p ON p.Id = bp.ProductionPlanId
LEFT JOIN dbo.location l ON l.Id = p.outletid
ORDER BY bp.Id DESC;";

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

            // ✅ BatchNo auto-generator (BP-0001)
            const string getNextBatchNoSql = @"
DECLARE @next INT = 1;

SELECT @next = ISNULL(MAX(TRY_CONVERT(INT, REPLACE(BatchNo,'BP-',''))),0) + 1
FROM dbo.BatchProduction WITH (UPDLOCK, HOLDLOCK)
WHERE BatchNo LIKE 'BP-%';

SELECT CONCAT('BP-', RIGHT('0000' + CAST(@next AS VARCHAR(10)), 4));";

            // 1) Create or Update batch header + lines
            int batchId;

            if (!req.Id.HasValue || req.Id.Value <= 0)
            {
                // ✅ if UI sends null/empty batchno, generate automatically
                var batchNo = string.IsNullOrWhiteSpace(req.BatchNo)
                    ? await conn.ExecuteScalarAsync<string>(getNextBatchNoSql, transaction: tx)
                    : req.BatchNo!.Trim();

                const string insertHeader = @"
INSERT INTO dbo.BatchProduction
(ProductionPlanId, WarehouseId, BatchNo, Status, CreatedBy, CreatedDate)
VALUES
(@ProductionPlanId, @WarehouseId, @BatchNo, 'Draft', @User, GETDATE());
SELECT CAST(SCOPE_IDENTITY() AS INT);";

                batchId = await conn.ExecuteScalarAsync<int>(insertHeader, new
                {
                    ProductionPlanId = req.ProductionPlanId,
                    WarehouseId = warehouseId.Value,
                    BatchNo = batchNo,
                    User = req.UserId
                }, tx);
            }
            else
            {
                batchId = req.Id.Value;

                const string lockBatch = @"
SELECT Status
FROM dbo.BatchProduction WITH (UPDLOCK, HOLDLOCK)
WHERE Id=@Id;";
                var st = await conn.ExecuteScalarAsync<string>(lockBatch, new { Id = batchId }, tx);

                if (st == null) throw new Exception("Batch not found");
                if (string.Equals(st, "Posted", StringComparison.OrdinalIgnoreCase))
                    throw new Exception("Already posted");

                // ✅ Do NOT overwrite BatchNo with NULL
                // If @BatchNo is null/empty => keep existing BatchNo
                const string updateHeader = @"
UPDATE dbo.BatchProduction
SET ProductionPlanId=@ProductionPlanId,
    WarehouseId=@WarehouseId,
    BatchNo = CASE 
                WHEN @BatchNo IS NULL OR LTRIM(RTRIM(@BatchNo)) = '' THEN BatchNo
                ELSE @BatchNo
              END,
    Status='Draft',
    UpdatedBy=@User,
    UpdatedDate=GETDATE()
WHERE Id=@Id;";

                await conn.ExecuteAsync(updateHeader, new
                {
                    Id = batchId,
                    ProductionPlanId = req.ProductionPlanId,
                    WarehouseId = warehouseId.Value,
                    BatchNo = req.BatchNo,
                    User = req.UserId
                }, tx);

                await conn.ExecuteAsync(
                    "DELETE FROM dbo.BatchProductionLines WHERE BatchProductionId=@Id;",
                    new { Id = batchId }, tx
                );
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

            var needs = (await conn.QueryAsync<(int ItemId, decimal RequiredQty)>(
                needSql, new { BatchId = batchId }, tx)).ToList();

            if (needs.Count == 0) throw new Exception("No ingredients found for recipes");

            // ==========================================================
            // 3) ✅ validate + reduce stock (SUPPLIER-WISE from ItemPrice)
            // ==========================================================
            foreach (var n in needs)
            {
                const string totalAvailSql = @"
SELECT CAST(ISNULL(SUM(CAST(Qty AS DECIMAL(18,4))),0) AS DECIMAL(18,4))
FROM dbo.ItemPrice WITH (UPDLOCK, HOLDLOCK)
WHERE ItemId=@ItemId AND WarehouseId=@WarehouseId AND ISNULL(Qty,0) > 0;";

                var totalAvail = await conn.ExecuteScalarAsync<decimal>(
                    totalAvailSql, new { n.ItemId, WarehouseId = warehouseId.Value }, tx);

                if (totalAvail < n.RequiredQty)
                    throw new Exception($"Insufficient stock for ItemId {n.ItemId}. Need {n.RequiredQty} but Available {totalAvail}");

                await ReduceItemPriceBySupplierAsync(
                    conn, tx,
                    itemId: n.ItemId,
                    warehouseId: warehouseId.Value,
                    requiredQty: n.RequiredQty,
                    userId: req.UserId
                );

                await ReduceItemWarehouseStockByBinAsync(
                    conn, tx,
                    itemId: n.ItemId,
                    warehouseId: warehouseId.Value,
                    requiredQty: n.RequiredQty,
                    userId: req.UserId
                );
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

            await conn.ExecuteAsync(markPosted, new { Id = batchId, User = req.UserId }, tx);

            tx.Commit();
            return batchId;
        }
        catch
        {
            tx.Rollback();
            throw;
        }
    }


    // ==========================================================
    // ✅ Supplier-wise reducer (ItemPrice)
    // ==========================================================
    private static async Task ReduceItemPriceBySupplierAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int itemId,
        int warehouseId,
        decimal requiredQty,
        int userId
    )
    {
        // NOTE:
        // You asked: "first reduced least supplier count"
        // so we order by Qty ASC (smallest first).
        // If you want FIFO, change ORDER BY CreatedDate ASC, Id ASC.

        const string pickLotsSql = @"
SELECT Id, SupplierId,
       CAST(Qty AS DECIMAL(18,4)) AS Qty
FROM dbo.ItemPrice WITH (UPDLOCK, ROWLOCK)
WHERE ItemId=@ItemId AND WarehouseId=@WarehouseId AND ISNULL(Qty,0) > 0
ORDER BY CAST(Qty AS DECIMAL(18,4)) ASC, Id ASC;";

        var lots = (await conn.QueryAsync<ItemPriceLotRow>(
            pickLotsSql,
            new { ItemId = itemId, WarehouseId = warehouseId },
            tx)).ToList();

        decimal remaining = requiredQty;

        const string reduceLotSql = @"
UPDATE dbo.ItemPrice
SET Qty = CAST(Qty AS DECIMAL(18,4)) - @Take,
    UpdatedBy = @User,
    UpdatedDate = GETDATE()
WHERE Id = @Id;";

        foreach (var lot in lots)
        {
            if (remaining <= 0) break;

            var take = lot.Qty >= remaining ? remaining : lot.Qty;
            if (take <= 0) continue;

            var aff = await conn.ExecuteAsync(reduceLotSql, new
            {
                Id = lot.Id,
                Take = take,
                User = userId
            }, tx);

            if (aff == 0)
                throw new Exception($"Failed to reduce ItemPrice lot Id={lot.Id} for ItemId={itemId}");

            remaining -= take;
        }

        if (remaining > 0)
            throw new Exception($"ItemPrice lots not enough for ItemId {itemId}. Remaining {remaining}");
    }


    private static async Task ReduceItemWarehouseStockByBinAsync(
    SqlConnection conn,
    SqlTransaction tx,
    int itemId,
    int warehouseId,
    decimal requiredQty,
    int userId
)
    {
        // Pick bins that have stock (least stock first)
        // If you want FIFO instead: ORDER BY CreatedDate ASC, Id ASC
        const string pickBinsSql = @"
SELECT Id,
       ISNULL(BinId, 0) AS BinId,
       CAST(ISNULL(OnHand,0) AS DECIMAL(18,4)) AS OnHand
FROM dbo.ItemWarehouseStock WITH (UPDLOCK, ROWLOCK)
WHERE ItemId=@ItemId
  AND WarehouseId=@WarehouseId
  AND ISNULL(OnHand,0) > 0
ORDER BY CAST(ISNULL(OnHand,0) AS DECIMAL(18,4)) ASC, Id ASC;";

        var bins = (await conn.QueryAsync<WsBinRow>(
            pickBinsSql,
            new { ItemId = itemId, WarehouseId = warehouseId },
            tx)).ToList();

        if (bins.Count == 0)
            throw new Exception($"ItemWarehouseStock has no bin stock for ItemId {itemId} WarehouseId {warehouseId}");

        decimal remaining = requiredQty;

        const string reduceBinSql = @"
UPDATE dbo.ItemWarehouseStock
SET OnHand = CAST(ISNULL(OnHand,0) AS DECIMAL(18,4)) - @Take,
    Available = CAST(ISNULL(Available,0) AS DECIMAL(18,4)) - @Take
WHERE Id = @Id;";

        foreach (var b in bins)
        {
            if (remaining <= 0) break;

            var take = b.OnHand >= remaining ? remaining : b.OnHand;
            if (take <= 0) continue;

            var aff = await conn.ExecuteAsync(reduceBinSql, new
            {
                Id = b.Id,
                Take = take
            }, tx);

            if (aff == 0)
                throw new Exception($"Failed to reduce ItemWarehouseStock bin row Id={b.Id} for ItemId={itemId}");

            remaining -= take;
        }

        if (remaining > 0)
            throw new Exception($"Bin stock not enough for ItemId {itemId}. Remaining {remaining}");
    }

    public async Task<IEnumerable<IngredientExplosionRowDto>> GetIngredientExplosionAsync(
     int recipeId, int warehouseId, decimal outputQty)
    {
        const string sql = @"
DECLARE @BaseOutput DECIMAL(18,4);

SELECT @BaseOutput = CAST(NULLIF(ExpectedOutput,0) AS DECIMAL(18,4))
FROM dbo.RecipeHeader
WHERE Id = @RecipeId;

IF (@BaseOutput IS NULL OR @BaseOutput = 0) SET @BaseOutput = 1;

;WITH need AS (
   SELECT
      ri.IngredientItemId,
      CAST(SUM(CAST(ri.Qty AS DECIMAL(18,4)) * (@OutputQty / @BaseOutput)) AS DECIMAL(18,4)) AS RequiredQty
   FROM dbo.RecipeIngredient ri
   WHERE ri.RecipeId = @RecipeId
   GROUP BY ri.IngredientItemId
),
stock AS (
   SELECT
     iws.ItemId,
     CAST(ISNULL(SUM(CAST(ISNULL(iws.Available,0) AS DECIMAL(18,4))),0) AS DECIMAL(18,4)) AS AvailableQty
   FROM dbo.ItemWarehouseStock iws
   WHERE iws.WarehouseId = @WarehouseId
   GROUP BY iws.ItemId
)
SELECT
  n.IngredientItemId,
  ISNULL(im.Name,'') AS IngredientName,
  n.RequiredQty,
  ISNULL(s.AvailableQty,0) AS AvailableQty,
  CASE WHEN ISNULL(s.AvailableQty,0) < n.RequiredQty THEN 'Shortage' ELSE 'OK' END AS Status
FROM need n
LEFT JOIN stock s ON s.ItemId = n.IngredientItemId
LEFT JOIN dbo.ItemMaster im ON im.Id = n.IngredientItemId
ORDER BY
  CASE WHEN ISNULL(s.AvailableQty,0) < n.RequiredQty THEN 0 ELSE 1 END,
  IngredientName ASC;
";

        // ✅ IMPORTANT: Use SAME parameter names as in SQL
        return await Connection.QueryAsync<IngredientExplosionRowDto>(sql, new
        {
            RecipeId = recipeId,
            WarehouseId = warehouseId,
            OutputQty = outputQty
        });
    }


    private sealed class WsBinRow
    {
        public int Id { get; set; }
        public int BinId { get; set; }
        public decimal OnHand { get; set; }
    }

    private sealed class ItemPriceLotRow
    {
        public int Id { get; set; }
        public int SupplierId { get; set; }
        public decimal Qty { get; set; }
    }

}
