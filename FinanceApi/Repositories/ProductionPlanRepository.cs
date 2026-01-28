using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Text.Json;

namespace FinanceApi.Repositories
{
    public class ProductionPlanRepository : DynamicRepository, IProductionPlanRepository
    {
        private readonly IDbConnection _db;
        public ProductionPlanRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory) { }

        public async Task<IEnumerable<SoHeaderDto>> GetSalesOrdersAsync(int? includeSoId = null)
        {
            const string sql = @"
SELECT TOP 200
    so.Id,
    so.SalesOrderNo,
    so.CustomerId,
    so.DeliveryDate,
    so.Status
FROM dbo.SalesOrder so
LEFT JOIN dbo.ProductionPlan pp
  ON pp.SalesOrderId = so.Id
 AND pp.Status <> 'Cancelled'           -- optional: ignore cancelled plans
WHERE ISNULL(so.IsActive, 1) = 1
  AND (
        pp.Id IS NULL                   -- not planned yet
        OR so.Id = @IncludeSoId         -- include edit SO
      )
ORDER BY so.Id DESC;
";

            return await Connection.QueryAsync<SoHeaderDto>(sql, new { IncludeSoId = includeSoId });
        }


        public async Task<ProductionPlanResponseDto> GetBySalesOrderAsync(int salesOrderId, int warehouseId)
        {
            using var multi = await Connection.QueryMultipleAsync(
                "dbo.sp_PP_GetBySalesOrder",
                new { SalesOrderId = salesOrderId, WarehouseId = warehouseId },
                commandType: CommandType.StoredProcedure);

            var plan = (await multi.ReadAsync<PlanRowDto>()).ToList();
            var ing = (await multi.ReadAsync<IngredientRowDto>()).ToList();

            return new ProductionPlanResponseDto { PlanRows = plan, Ingredients = ing };
        }

        public async Task<int> SavePlanAsync(SavePlanRequest req)
        {
            var planId = await Connection.ExecuteScalarAsync<int>(
                "dbo.sp_PP_SavePlan",
                new
                {
                    SalesOrderId = req.SalesOrderId,
                    OutletId = req.OutletId,
                    WarehouseId = req.WarehouseId,
                    CreatedBy = req.CreatedBy
                },
                commandType: CommandType.StoredProcedure);

            return planId;
        }
        public async Task<List<ProductionPlanListDto>> ListPlansWithLinesAsync()
        {
            using var multi = await Connection.QueryMultipleAsync(
                "dbo.sp_PP_ListPlans_WithLines",
                commandType: CommandType.StoredProcedure
            );

            var plans = (await multi.ReadAsync<ProductionPlanListDto>()).ToList();
            var lines = (await multi.ReadAsync<ProductionPlanLineDto>()).ToList();

            var dict = plans.ToDictionary(x => x.Id, x => x);
            foreach (var ln in lines)
            {
                if (dict.TryGetValue(ln.ProductionPlanId, out var p))
                    p.Lines.Add(ln);
            }

            return plans;
        }

        public async Task<IEnumerable<ShortageGrnAlertDto>> GetShortageGrnAlertsAsync()
        {
            const string sql = @"
SELECT
    p.Id            AS ProductionPlanId,
    p.SalesOrderId  AS SalesOrderId,

    pg.Id           AS GrnId,
    pg.GrnNo        AS GrnNo,
    pg.ReceptionDate,

    gd.ItemCode     AS ItemCode,
    it.ItemName     AS ItemName,
    CAST(ISNULL(gd.QtyReceived, 0) AS decimal(18,2)) AS QtyReceived
FROM dbo.ProductionPlan p
JOIN dbo.PurchaseGoodReceipt pg
  ON pg.IsActive = 1
 AND pg.SourceType = 'RECIPE_SHORTAGE'
 AND pg.SourceRefId = p.Id              -- ✅ IMPORTANT FIX (use PlanId)
OUTER APPLY OPENJSON(pg.GRNJson)
WITH (
    ItemCode     nvarchar(50)  '$.itemCode',
    QtyReceived  decimal(18,2) '$.qtyReceived'
) AS gd
LEFT JOIN dbo.Item it
  ON it.ItemCode = gd.ItemCode
ORDER BY pg.Id DESC;";

            return await Connection.QueryAsync<ShortageGrnAlertDto>(sql);
        }
        public async Task<int> UpdateAsync(ProductionPlanUpdateRequest req)
        {
            var linesJson = JsonSerializer.Serialize(req.Lines ?? new());

            var pid = await Connection.ExecuteScalarAsync<int>(
                "[dbo].[sp_PP_UpdatePlan]",
                new
                {
                    req.Id,
                    req.SalesOrderId,
                    req.OutletId,
                    req.WarehouseId,
                    PlanDate = req.PlanDate.Date,
                    req.Status,
                    UpdatedBy = req.UpdatedBy,
                    LinesJson = linesJson
                },
                commandType: CommandType.StoredProcedure
            );

            return pid;
        }

        public async Task<int> DeleteAsync(int id)
        {
            var pid = await Connection.ExecuteScalarAsync<int>(
                "[dbo].[sp_PP_DeletePlan]",
                new { Id = id },
                commandType: CommandType.StoredProcedure
            );

            return pid;
        }
        public async Task<ProductionPlanGetByIdDto> GetByIdAsync(int id)
        {
            using var conn = (SqlConnection)Connection;
            if (conn.State != ConnectionState.Open)
                await conn.OpenAsync();

            const string sql = @"
-- 1) Header
SELECT TOP 1
  p.Id,
  p.SalesOrderId,
  p.OutletId,
  p.WarehouseId,
  p.PlanDate,
  p.Status,
  p.CreatedBy,
  p.CreatedDate
FROM [Finance].[dbo].[ProductionPlan] p
WHERE p.Id = @Id;

-- 2) Lines
SELECT
  pl.Id,
  pl.ProductionPlanId,
  pl.RecipeId,
  pl.FinishedItemId,
  CAST(pl.PlannedQty AS DECIMAL(18,4)) AS PlannedQty,
  CAST(pl.ExpectedOutput AS DECIMAL(18,4)) AS ExpectedOutput,
  im.Name AS FinishedItemName
FROM [dbo].[ProductionPlanLines] pl
LEFT JOIN [dbo].[ItemMaster] im ON im.Id = pl.FinishedItemId
WHERE pl.ProductionPlanId = @Id
ORDER BY pl.Id;
";

            using var multi = await conn.QueryMultipleAsync(sql, new { Id = id });

            var header = await multi.ReadFirstOrDefaultAsync<ProductionPlanHeaderDto>();
            if (header == null)
                throw new Exception("Production plan not found");

            var lines = (await multi.ReadAsync<ProductionPlanLineDto>()).AsList();

            return new ProductionPlanGetByIdDto
            {
                Header = header,
                Lines = lines
            };
        }

    }
}
