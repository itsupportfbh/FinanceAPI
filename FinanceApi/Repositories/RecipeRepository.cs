using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using System.Data;

namespace FinanceApi.Repositories
{
    public class RecipeRepository : IRecipeRepository
    {
        private readonly IDbConnectionFactory _connectionFactory;
        public RecipeRepository(IDbConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        private IDbConnection Connection => _connectionFactory.CreateConnection();

        // ---------- CREATE ----------
        public async Task<int> CreateAsync(RecipeCreateDto dto, string? createdBy)
        {
            using var conn = Connection;
            conn.Open();
            using var tx = conn.BeginTransaction();

            try
            {
                // ✅ UNIQUE constraint guard (UQ_RecipeHeader_FinishedItem)
                var exists = await conn.ExecuteScalarAsync<int>(@"
SELECT COUNT(1) FROM RecipeHeader WHERE FinishedItemId = @FinishedItemId;",
                    new { dto.FinishedItemId }, tx);

                if (exists > 0)
                    throw new InvalidOperationException("Recipe already exists for this Finished Item.");

                var totals = CalcTotals(dto);

                // Header
                var recipeId = await conn.ExecuteScalarAsync<int>(@"
INSERT INTO RecipeHeader
(FinishedItemId, Cuisine, Status, YieldPct, BatchQty, BatchUom, Notes,
 TotalCost, ExpectedOutput, CostPerUnit, CreatedBy, CreatedDate)
VALUES
(@FinishedItemId, @Cuisine, @Status, @YieldPct, @BatchQty, @BatchUom, @Notes,
 @TotalCost, @ExpectedOutput, @CostPerUnit, @CreatedBy, GETDATE());
SELECT CAST(SCOPE_IDENTITY() AS INT);",
                new
                {
                    dto.FinishedItemId,
                    dto.Cuisine,
                    dto.Status,
                    dto.YieldPct,
                    dto.BatchQty,
                    dto.BatchUom,
                    dto.Notes,
                    TotalCost = totals.TotalCost,
                    ExpectedOutput = totals.ExpectedOutput,
                    CostPerUnit = totals.CostPerUnit,
                    CreatedBy = createdBy
                }, tx);

                // Lines
                const string sqlLine = @"
INSERT INTO RecipeIngredient
(RecipeId, IngredientItemId, Qty, Uom, YieldPct, UnitCost, RowCost, SortOrder, CreatedDate, Remarks)
VALUES
(@RecipeId, @IngredientItemId, @Qty, @Uom, @YieldPct, @UnitCost, @RowCost, @SortOrder, GETDATE(), @Remarks);";

                int sort = 1;
                foreach (var l in dto.Ingredients)
                {
                    var rowCost = CalcRowCost(l.Qty, l.YieldPct, l.UnitCost);

                    await conn.ExecuteAsync(sqlLine, new
                    {
                        RecipeId = recipeId,
                        l.IngredientItemId,
                        l.Qty,
                        Uom = l.Uom,
                        l.YieldPct,
                        l.UnitCost,
                        RowCost = rowCost,
                        l.Remarks,
                        SortOrder = l.SortOrder > 0 ? l.SortOrder : sort
                    }, tx);

                    sort++;
                }

                tx.Commit();
                return recipeId;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ---------- UPDATE (already shared earlier, keep) ----------
        public async Task<int> UpdateAsync(int recipeId, RecipeUpdateDto dto, string updatedBy)
        {
            using var conn = Connection;
            conn.Open();
            using var tx = conn.BeginTransaction();

            try
            {
                var exists = await conn.ExecuteScalarAsync<int>(@"
SELECT COUNT(1) FROM RecipeHeader WHERE Id = @Id;",
                    new { Id = recipeId }, tx);

                if (exists == 0) return 0;

                // ✅ if FinishedItemId changed, check UNIQUE
                var clash = await conn.ExecuteScalarAsync<int>(@"
SELECT COUNT(1) FROM RecipeHeader 
WHERE FinishedItemId = @FinishedItemId AND Id <> @Id;",
                    new { dto.FinishedItemId, Id = recipeId }, tx);

                if (clash > 0)
                    throw new InvalidOperationException("Another recipe already exists for this Finished Item.");

                // totals
                var totals = CalcTotals(new RecipeCreateDto
                {
                    FinishedItemId = dto.FinishedItemId,
                    Cuisine = dto.Cuisine,
                    Status = dto.Status,
                    YieldPct = dto.YieldPct,
                    BatchQty = dto.BatchQty,
                    BatchUom = dto.BatchUom,
                    Notes = dto.Notes,
                    Ingredients = dto.Ingredients
                });

                await conn.ExecuteAsync(@"
UPDATE RecipeHeader
SET FinishedItemId = @FinishedItemId,
    Cuisine        = @Cuisine,
    Status         = @Status,
    YieldPct       = @YieldPct,
    BatchQty       = @BatchQty,
    BatchUom       = @BatchUom,
    Notes          = @Notes,
    TotalCost      = @TotalCost,
    ExpectedOutput = @ExpectedOutput,
    CostPerUnit    = @CostPerUnit,
    UpdatedBy      = @UpdatedBy,
    UpdatedDate    = GETDATE()
WHERE Id = @Id;",
                    new
                    {
                        Id = recipeId,
                        dto.FinishedItemId,
                        dto.Cuisine,
                        dto.Status,
                        dto.YieldPct,
                        dto.BatchQty,
                        dto.BatchUom,
                        dto.Notes,
                        totals.TotalCost,
                        totals.ExpectedOutput,
                        totals.CostPerUnit,
                        UpdatedBy = updatedBy
                    }, tx);

                await conn.ExecuteAsync(@"DELETE FROM RecipeIngredient WHERE RecipeId=@RecipeId;",
                    new { RecipeId = recipeId }, tx);

                const string sqlLine = @"
INSERT INTO RecipeIngredient
(RecipeId, IngredientItemId, Qty, Uom, YieldPct, UnitCost, RowCost, SortOrder, CreatedDate, Remarks)
VALUES
(@RecipeId, @IngredientItemId, @Qty, @Uom, @YieldPct, @UnitCost, @RowCost, @SortOrder, GETDATE(), @Remarks);";

                int sort = 1;
                foreach (var l in dto.Ingredients)
                {
                    var rowCost = CalcRowCost(l.Qty, l.YieldPct, l.UnitCost);

                    await conn.ExecuteAsync(sqlLine, new
                    {
                        RecipeId = recipeId,
                        l.IngredientItemId,
                        l.Qty,
                        Uom = l.Uom,
                        l.YieldPct,
                        l.UnitCost,
                        RowCost = rowCost,
                        l.Remarks,
                        SortOrder = l.SortOrder > 0 ? l.SortOrder : sort
                    }, tx);

                    sort++;
                }

                tx.Commit();
                return recipeId;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ---------- LIST ----------
        public async Task<IEnumerable<RecipeListDto>> ListAsync()
        {
            using var conn = Connection;

            const string sql = @"
SELECT rh.Id,
       rh.FinishedItemId,
       i.ItemCode AS FinishedItemCode,
       i.ItemName AS FinishedItemName,
       rh.Status,
       rh.Cuisine,
       rh.TotalCost,
       rh.CreatedDate
FROM RecipeHeader rh
INNER JOIN Item i ON i.Id = rh.FinishedItemId
WHERE ISNULL(rh.Status,'') <> 'Deleted'
ORDER BY rh.Id DESC;";

            return await conn.QueryAsync<RecipeListDto>(sql);
        }

        // ---------- GET BY ID ----------
        public async Task<RecipeReadDto?> GetByIdAsync(int id)
        {
            using var conn = Connection;

            const string sql = @"
SELECT  rh.Id,
        rh.FinishedItemId,
        i.ItemCode AS FinishedItemCode,
        i.ItemName AS FinishedItemName,
        COALESCE(u.Name,'') AS FinishedUomName,
        rh.Status,
        rh.Cuisine,
        rh.YieldPct,
        rh.BatchQty,
        rh.BatchUom,
        rh.Notes,
        rh.TotalCost,
        rh.ExpectedOutput,
        rh.CostPerUnit
FROM RecipeHeader rh
INNER JOIN Item i ON i.Id = rh.FinishedItemId
LEFT JOIN Uom u ON u.Id = i.UomId
WHERE rh.Id = @Id;

SELECT  ri.Id,
        ri.IngredientItemId,
        im.ItemCode AS IngredientItemCode,
        im.ItemName AS IngredientItemName,
        COALESCE(u2.Name,'') AS IngredientUomName,
        ri.Qty,
        ri.Uom,
        ri.YieldPct,
        ri.UnitCost,
        ri.RowCost,
        ri.SortOrder,
        ri.CreatedDate,
        ri.Remarks
FROM RecipeIngredient ri
INNER JOIN Item im ON im.Id = ri.IngredientItemId
LEFT JOIN Uom u2 ON u2.Id = im.UomId
WHERE ri.RecipeId = @Id
ORDER BY ri.SortOrder, ri.Id;";

            using var multi = await conn.QueryMultipleAsync(sql, new { Id = id });

            var header = await multi.ReadFirstOrDefaultAsync<RecipeReadDto>();
            if (header == null) return null;

            header.Ingredients = (await multi.ReadAsync<RecipeIngredientReadDto>()).ToList();
            return header;
        }

        // ---------- DELETE (status only) ----------
        public async Task<bool> DeleteAsync(int id, string deletedBy)
        {
            using var conn = Connection;

            var rows = await conn.ExecuteAsync(@"
UPDATE RecipeHeader
SET Status='Deleted', UpdatedBy=@User, UpdatedDate=GETDATE()
WHERE Id=@Id;",
            new { Id = id, User = deletedBy });

            return rows > 0;
        }

        // ===== helpers =====
        private static decimal CalcRowCost(decimal qty, decimal yieldPct, decimal unitCost)
        {
            var y = yieldPct <= 0 ? 100 : yieldPct;
            var reqQty = qty / (y / 100m);
            return Math.Round(reqQty * unitCost, 4);
        }

        private static (decimal TotalCost, decimal ExpectedOutput, decimal CostPerUnit) CalcTotals(RecipeCreateDto dto)
        {
            decimal total = 0;
            foreach (var l in dto.Ingredients)
                total += CalcRowCost(l.Qty, l.YieldPct, l.UnitCost);

            var y = dto.YieldPct < 0 ? 0 : (dto.YieldPct > 100 ? 100 : dto.YieldPct);
            var expectedOutput = Math.Round(dto.BatchQty * (y / 100m), 4);

            var cpu = expectedOutput > 0 ? Math.Round(total / expectedOutput, 4) : 0;
            total = Math.Round(total, 4);

            return (total, expectedOutput, cpu);
        }
    }
}
