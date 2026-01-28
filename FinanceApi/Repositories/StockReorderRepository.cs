using System.Data;
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.Data.SqlClient;

namespace FinanceApi.Repositories
{
    public class StockReorderRepository : DynamicRepository, IStockReorderRepository
    {
        private readonly ApplicationDbContext _context;

        public StockReorderRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
        }

        // --------------------------------------------------------------------
        // LIST (headers + lines)
        // --------------------------------------------------------------------
        public async Task<IEnumerable<StockReorderDTO>> GetAllAsync()
        {
            const string headersSql = @"
SELECT
    sr.Id,
    sr.WarehouseTypeId,
    w.Name AS WarehouseName,
    sr.MethodId,
    sr.HorizonDays      AS Horizon,
    sr.IncludeLeadTime  AS LeadTime,
    sr.Status,
    sr.CreatedBy,
    sr.CreatedDate,
    sr.UpdatedBy,
    sr.UpdatedDate,
    sr.IsActive
FROM dbo.StockReorder sr
LEFT JOIN dbo.Warehouse w ON w.Id = sr.WarehouseTypeId
WHERE sr.IsActive = 1
ORDER BY sr.Id;";

            var headers = (await Connection.QueryAsync<StockReorderDTO>(headersSql)).ToList();
            if (headers.Count == 0) return headers;

            var reorderIds = headers.Select(h => h.Id).ToArray();

            const string linesSql = @"
SELECT
    Id, StockReorderId, WarehouseTypeId, ItemId, OnHand, [Min] AS Min, [Max] AS Max,
    ReorderQty, LeadDays, UsageHorizon, Suggested, Status, Selected,
    CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
FROM dbo.StockReorderLines
WHERE StockReorderId IN @Ids AND IsActive = 1;";

            var lines = (await Connection.QueryAsync<StockReorderLines>(linesSql, new { Ids = reorderIds })).ToList();
            if (lines.Count == 0) return headers;

            var lineIds = lines.Select(l => l.Id).ToArray();

            const string suppliersSql = @"
SELECT
    Id, StockReorderLineId, SupplierId, Price, Qty, Selected, IsActive
FROM dbo.StockReorderLineSuppliers
WHERE StockReorderLineId IN @LineIds AND IsActive = 1;";

            var suppliers = await Connection.QueryAsync<StockReorderLineSupplier>(suppliersSql, new { LineIds = lineIds });

            var headerById = headers.ToDictionary(h => h.Id);
            var lineById = lines.ToDictionary(l => l.Id);

            foreach (var ln in lines)
                if (headerById.TryGetValue(ln.StockReorderId, out var parent))
                    (parent.LineItems ??= new List<StockReorderLines>()).Add(ln);

            foreach (var s in suppliers)
                if (lineById.TryGetValue(s.StockReorderLineId, out var ln))
                    (ln.SupplierBreakdown ??= new List<StockReorderLineSupplier>()).Add(s);

            return headers;
        }

        public async Task<IEnumerable<ReorderPreviewLine>> GetReorderPreviewAsync(int stockReorderId)
        {
            const string sql = @"
WITH PRs AS (
    SELECT p.Id, p.PurchaseRequestNo, p.StockReorderId, p.PRLines,p.Status
    FROM dbo.PurchaseRequest p
    WHERE p.IsActive = 1
      AND p.IsReorder = 1
      AND p.StockReorderId = @StockReorderId
)
SELECT
    pr.Status                          AS Status,
    pr.PurchaseRequestNo           AS PrNo,
    j.itemId                       AS ItemId,
    j.itemCode                     AS ItemCode,
    COALESCE(j.itemName, j.itemSearch, '') AS ItemName,
    j.qty                          AS RequestedQty,
    j.supplierId                   AS SupplierId,
    j.warehouseId                  AS WarehouseId,
    j.location                     AS Location,
    TRY_CONVERT(date, j.deliveryDate) AS DeliveryDate,
    ISNULL(srl.OnHand,0)           AS OnHand,
    ISNULL(srl.[Min],0)            AS MinQty,
    ISNULL(srl.[Max],0)            AS MaxQty,
    ISNULL(srl.ReorderQty,0)       AS ReorderQty
FROM PRs pr
CROSS APPLY OPENJSON(pr.PRLines)
WITH (
    itemId       int            '$.itemId',
    itemCode     nvarchar(100)  '$.itemCode',
    itemName     nvarchar(200)  '$.itemName',
    itemSearch   nvarchar(200)  '$.itemSearch',
    qty          decimal(18,4)  '$.qty',
    supplierId   int            '$.supplierId',
    warehouseId  int            '$.warehouseId',
    location     nvarchar(200)  '$.location',
    deliveryDate nvarchar(40)   '$.deliveryDate'
) AS j
LEFT JOIN dbo.StockReorderLines srl
       ON srl.StockReorderId = pr.StockReorderId
      AND srl.ItemId         = j.itemId
WHERE ISNULL(j.itemId,0) > 0;";

            return await Connection.QueryAsync<ReorderPreviewLine>(sql, new { StockReorderId = stockReorderId });
        }

        // --------------------------------------------------------------------
        // GET BY ID (header + lines)
        // --------------------------------------------------------------------
        public async Task<StockReorderDTO?> GetByIdAsync(int id)
        {
            const string headerSql = @"
SELECT TOP (1)
    sr.Id,
    sr.WarehouseTypeId,
    w.Name AS WarehouseName,
    sr.MethodId,
    sr.HorizonDays,
    sr.IncludeLeadTime,
    sr.Status,
    sr.CreatedBy,
    sr.CreatedDate,
    sr.UpdatedBy,
    sr.UpdatedDate,
    sr.IsActive
FROM dbo.StockReorder sr
LEFT JOIN dbo.Warehouse w ON w.Id = sr.WarehouseTypeId
WHERE sr.Id = @Id AND sr.IsActive = 1;";

            var header = await Connection.QueryFirstOrDefaultAsync<StockReorderDTO>(headerSql, new { Id = id });
            if (header == null) return null;

            const string linesSql = @"
SELECT
    Id, StockReorderId, WarehouseTypeId, ItemId, OnHand, [Min] AS Min, [Max] AS Max,
    ReorderQty, LeadDays, UsageHorizon, Suggested, Status, Selected,
    CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
FROM dbo.StockReorderLines
WHERE StockReorderId = @Id AND IsActive = 1
ORDER BY Id;";

            var lines = (await Connection.QueryAsync<StockReorderLines>(linesSql, new { Id = id })).ToList();
            header.LineItems = lines;

            if (lines.Count > 0)
            {
                var lineIds = lines.Select(l => l.Id).ToArray();
                const string suppliersSql = @"
SELECT
    Id, StockReorderLineId, SupplierId, Price, Qty, Selected, IsActive
FROM dbo.StockReorderLineSuppliers
WHERE StockReorderLineId IN @LineIds AND IsActive = 1;";
                var suppliers = await Connection.QueryAsync<StockReorderLineSupplier>(suppliersSql, new { LineIds = lineIds });

                var lineById = lines.ToDictionary(l => l.Id);
                foreach (var s in suppliers)
                {
                    if (lineById.TryGetValue(s.StockReorderLineId, out var ln))
                    {
                        ln.SupplierBreakdown ??= new List<StockReorderLineSupplier>();
                        ln.SupplierBreakdown.Add(s);
                    }
                }
            }

            return header;
        }


        // --------------------------------------------------------------------
        // CREATE (header + lines)
        // --------------------------------------------------------------------
        public async Task<int> CreateAsync(StockReorder stockReorder)
        {
            if (stockReorder is null) throw new ArgumentNullException(nameof(stockReorder));

            var now = DateTime.UtcNow;
            if (stockReorder.CreatedDate == default) stockReorder.CreatedDate = now;
            if (stockReorder.UpdatedDate == default) stockReorder.UpdatedDate = now;

            const string insertHeaderSql = @"
INSERT INTO dbo.StockReorder
(
    WarehouseTypeId, MethodId, HorizonDays, IncludeLeadTime,
    Status, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
OUTPUT INSERTED.Id
VALUES
(
    @WarehouseTypeId, @MethodId, @HorizonDays, @IncludeLeadTime,
    @Status, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive
);";

            // NOTE: we need OUTPUT to capture line Ids
            const string insertLineSql = @"
INSERT INTO dbo.StockReorderLines
(
    StockReorderId, WarehouseTypeId, ItemId,
    OnHand, [Min], [Max], ReorderQty, LeadDays, UsageHorizon, Suggested,
    Status, Selected, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
OUTPUT INSERTED.Id
VALUES
(
    @StockReorderId, @WarehouseTypeId, @ItemId,
    @OnHand, @Min, @Max, @ReorderQty, @LeadDays, @UsageHorizon, @Suggested,
    @Status, @Selected, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1
);";

            const string insertSupplierSql = @"
INSERT INTO dbo.StockReorderLineSuppliers
(
    StockReorderLineId, SupplierId, Price, Qty, Selected,
    CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
VALUES
(
    @StockReorderLineId, @SupplierId, @Price, @Qty, @Selected,
    @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1
);";

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();

            try
            {
                var newId = await conn.ExecuteScalarAsync<int>(
                    insertHeaderSql,
                    new
                    {
                        stockReorder.WarehouseTypeId,
                        stockReorder.MethodId,
                        stockReorder.HorizonDays,
                        stockReorder.IncludeLeadTime,
                        stockReorder.Status,
                        stockReorder.CreatedBy,
                        stockReorder.CreatedDate,
                        stockReorder.UpdatedBy,
                        stockReorder.UpdatedDate,
                        IsActive = stockReorder.IsActive
                    },
                    tx);

                if (stockReorder.LineItems?.Count > 0)
                {
                    foreach (var l in stockReorder.LineItems)
                    {
                        // 1) insert the line and capture Id
                        var lineId = await conn.ExecuteScalarAsync<int>(
                            insertLineSql,
                            new
                            {
                                StockReorderId = newId,
                                l.WarehouseTypeId,
                                l.ItemId,
                                l.OnHand,
                                Min = l.Min,
                                Max = l.Max,
                                l.ReorderQty,
                                l.LeadDays,
                                l.UsageHorizon,
                                l.Suggested,
                                l.Status,
                                l.Selected,
                                CreatedBy = stockReorder.CreatedBy,
                                CreatedDate = stockReorder.CreatedDate,
                                UpdatedBy = stockReorder.UpdatedBy,
                                UpdatedDate = stockReorder.UpdatedDate
                            },
                            tx);

                        // 2) insert supplier breakdown rows for this line
                        if (l.SupplierBreakdown != null && l.SupplierBreakdown.Count > 0)
                        {
                            var sParams = l.SupplierBreakdown.Select(s => new
                            {
                                StockReorderLineId = lineId,
                                s.SupplierId,
                                s.Price,
                                s.Qty,
                                s.Selected,
                                CreatedBy = stockReorder.CreatedBy,
                                CreatedDate = stockReorder.CreatedDate,
                                UpdatedBy = stockReorder.UpdatedBy,
                                UpdatedDate = stockReorder.UpdatedDate
                            });

                            await conn.ExecuteAsync(insertSupplierSql, sParams, tx);
                        }
                    }
                }

                tx.Commit();
                return newId;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }


        // --------------------------------------------------------------------
        // UPDATE (header + upsert lines + soft delete missing)
        // --------------------------------------------------------------------
        public async Task UpdateAsync(StockReorder stockReorder)
        {
            if (stockReorder is null) throw new ArgumentNullException(nameof(stockReorder));

            var now = DateTime.UtcNow;
            stockReorder.UpdatedDate = now;

            const string updateHeaderSql = @"
UPDATE dbo.StockReorder
SET WarehouseTypeId=@WarehouseTypeId, MethodId=@MethodId, HorizonDays=@HorizonDays,
    IncludeLeadTime=@IncludeLeadTime, Status=@Status,
    UpdatedBy=@UpdatedBy, UpdatedDate=@UpdatedDate
WHERE Id=@Id;";

            const string updateLineSql = @"
UPDATE dbo.StockReorderLines
SET WarehouseTypeId=@WarehouseTypeId, ItemId=@ItemId, OnHand=@OnHand,
    [Min]=@Min, [Max]=@Max, ReorderQty=@ReorderQty, LeadDays=@LeadDays,
    UsageHorizon=@UsageHorizon, Suggested=@Suggested, Status=@Status,
    Selected=@Selected, UpdatedBy=@UpdatedBy, UpdatedDate=@UpdatedDate, IsActive=1
WHERE Id=@Id AND StockReorderId=@StockReorderId;";

            const string insertLineSql = @"
INSERT INTO dbo.StockReorderLines
(StockReorderId, WarehouseTypeId, ItemId, OnHand, [Min], [Max], ReorderQty, LeadDays, UsageHorizon, Suggested,
 Status, Selected, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive)
VALUES
(@StockReorderId, @WarehouseTypeId, @ItemId, @OnHand, @Min, @Max, @ReorderQty, @LeadDays, @UsageHorizon, @Suggested,
 @Status, @Selected, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1);
SELECT CAST(SCOPE_IDENTITY() AS INT);";

            // Supplier upsert
            const string updateSupplierSql = @"
UPDATE dbo.StockReorderLineSuppliers
SET SupplierId=@SupplierId, Price=@Price, Qty=@Qty, Selected=@Selected,
    UpdatedBy=@UpdatedBy, UpdatedDate=@UpdatedDate, IsActive=1
WHERE Id=@Id AND StockReorderLineId=@StockReorderLineId;";

            const string insertSupplierSql = @"
INSERT INTO dbo.StockReorderLineSuppliers
(StockReorderLineId, SupplierId, Price, Qty, Selected, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive)
VALUES
(@StockReorderLineId, @SupplierId, @Price, @Qty, @Selected, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1);
SELECT CAST(SCOPE_IDENTITY() AS INT);";

            const string softDeleteMissingSuppliersSql = @"
UPDATE dbo.StockReorderLineSuppliers
SET IsActive=0, UpdatedBy=@UpdatedBy, UpdatedDate=@UpdatedDate
WHERE StockReorderLineId=@LineId AND IsActive=1
  AND (@KeepIdsCount = 0 OR Id NOT IN @KeepIds);";

            const string softDeleteMissingLinesSql = @"
UPDATE dbo.StockReorderLines
SET IsActive=0, UpdatedBy=@UpdatedBy, UpdatedDate=@UpdatedDate
WHERE StockReorderId=@StockReorderId AND IsActive=1
  AND (@KeepIdsCount = 0 OR Id NOT IN @KeepIds);";

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();

            try
            {
                // Header
                await conn.ExecuteAsync(updateHeaderSql, new
                {
                    stockReorder.WarehouseTypeId,
                    stockReorder.MethodId,
                    stockReorder.HorizonDays,
                    stockReorder.IncludeLeadTime,
                    stockReorder.Status,
                    stockReorder.UpdatedBy,
                    UpdatedDate = now,
                    stockReorder.Id
                }, tx);

                // Upsert lines + suppliers
                var keepLineIds = new List<int>();

                if (stockReorder.LineItems?.Count > 0)
                {
                    foreach (var l in stockReorder.LineItems)
                    {
                        int lineId = l.Id;

                        if (l.Id > 0)
                        {
                            await conn.ExecuteAsync(updateLineSql, new
                            {
                                l.Id,
                                StockReorderId = stockReorder.Id,
                                l.WarehouseTypeId,
                                l.ItemId,
                                l.OnHand,
                                Min = l.Min,
                                Max = l.Max,
                                l.ReorderQty,
                                l.LeadDays,
                                l.UsageHorizon,
                                l.Suggested,
                                l.Status,
                                l.Selected,
                                UpdatedBy = stockReorder.UpdatedBy,
                                UpdatedDate = now
                            }, tx);
                        }
                        else
                        {
                            lineId = await conn.ExecuteScalarAsync<int>(insertLineSql, new
                            {
                                StockReorderId = stockReorder.Id,
                                l.WarehouseTypeId,
                                l.ItemId,
                                l.OnHand,
                                Min = l.Min,
                                Max = l.Max,
                                l.ReorderQty,
                                l.LeadDays,
                                l.UsageHorizon,
                                l.Suggested,
                                l.Status,
                                l.Selected,
                                CreatedBy = stockReorder.UpdatedBy ?? stockReorder.CreatedBy,
                                CreatedDate = now,
                                UpdatedBy = stockReorder.UpdatedBy,
                                UpdatedDate = now
                            }, tx);
                        }

                        keepLineIds.Add(lineId);

                        // === Suppliers for this line ===
                        var keepSupplierIds = new List<int>();

                        if (l.SupplierBreakdown != null && l.SupplierBreakdown.Count > 0)
                        {
                            foreach (var s in l.SupplierBreakdown)
                            {
                                if (s.Id > 0)
                                {
                                    await conn.ExecuteAsync(updateSupplierSql, new
                                    {
                                        s.Id,
                                        StockReorderLineId = lineId,
                                        s.SupplierId,
                                        s.Price,
                                        s.Qty,
                                        s.Selected,
                                        UpdatedBy = stockReorder.UpdatedBy,
                                        UpdatedDate = now
                                    }, tx);

                                    keepSupplierIds.Add(s.Id);
                                }
                                else
                                {
                                    var newSupplierId = await conn.ExecuteScalarAsync<int>(insertSupplierSql, new
                                    {
                                        StockReorderLineId = lineId,
                                        s.SupplierId,
                                        s.Price,
                                        s.Qty,
                                        s.Selected,
                                        CreatedBy = stockReorder.UpdatedBy ?? stockReorder.CreatedBy,
                                        CreatedDate = now,
                                        UpdatedBy = stockReorder.UpdatedBy,
                                        UpdatedDate = now
                                    }, tx);

                                    keepSupplierIds.Add(newSupplierId);
                                }
                            }
                        }

                        // Soft delete suppliers not sent back
                        await conn.ExecuteAsync(softDeleteMissingSuppliersSql, new
                        {
                            LineId = lineId,
                            KeepIds = keepSupplierIds.Count == 0 ? new[] { -1 } : keepSupplierIds.ToArray(),
                            KeepIdsCount = keepSupplierIds.Count,
                            UpdatedBy = stockReorder.UpdatedBy,
                            UpdatedDate = now
                        }, tx);
                    }
                }

                // Soft delete lines not sent back
                await conn.ExecuteAsync(softDeleteMissingLinesSql, new
                {
                    StockReorderId = stockReorder.Id,
                    KeepIds = keepLineIds.Count == 0 ? new[] { -1 } : keepLineIds.ToArray(),
                    KeepIdsCount = keepLineIds.Count,
                    UpdatedBy = stockReorder.UpdatedBy,
                    UpdatedDate = now
                }, tx);

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }


        // --------------------------------------------------------------------
        // DEACTIVATE (header + lines)
        // --------------------------------------------------------------------
        public async Task DeactivateAsync(int id, int updatedBy)
        {
            const string sqlHeader = @"
UPDATE dbo.StockReorder
SET IsActive = 0, UpdatedBy = @UpdatedBy, UpdatedDate = SYSUTCDATETIME()
WHERE Id = @Id;";

            const string sqlLines = @"
UPDATE dbo.StockReorderLines
SET IsActive = 0, UpdatedBy = @UpdatedBy, UpdatedDate = SYSUTCDATETIME()
WHERE StockReorderId = @Id AND IsActive = 1;";

            const string sqlSuppliers = @"
UPDATE s
SET s.IsActive = 0, s.UpdatedBy = @UpdatedBy, s.UpdatedDate = SYSUTCDATETIME()
FROM dbo.StockReorderLineSuppliers s
JOIN dbo.StockReorderLines l ON l.Id = s.StockReorderLineId
WHERE l.StockReorderId = @Id AND s.IsActive = 1;";

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();

            try
            {
                var affectedHeader = await conn.ExecuteAsync(sqlHeader, new { Id = id, UpdatedBy = updatedBy }, tx);
                if (affectedHeader == 0)
                    throw new KeyNotFoundException("StockReorder not found.");

                await conn.ExecuteAsync(sqlSuppliers, new { Id = id, UpdatedBy = updatedBy }, tx);
                await conn.ExecuteAsync(sqlLines, new { Id = id, UpdatedBy = updatedBy }, tx);

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }


        // --------------------------------------------------------------------
        // WAREHOUSE ITEMS (source rows for planning grid)
        // --------------------------------------------------------------------
        public async Task<IEnumerable<StockReorderWarehouseItems>> GetWarehouseItemsAsync(long warehouseId)
        {
            const string sql = @"
WITH ipx AS (
    SELECT ip.*,
           ROW_NUMBER() OVER (
               PARTITION BY ip.ItemId, ip.SupplierId, ip.WarehouseId
               ORDER BY ISNULL(ip.UpdatedDate, ip.CreatedDate) DESC, ip.Id DESC
           ) AS rn
    FROM dbo.ItemPrice ip
)
SELECT
    im.Id                    AS ItemId,
    im.Name                  AS ItemName,
    im.Sku                   AS Sku,
    iws.WarehouseId          AS WarehouseId,
    w.Name                   AS WarehouseName,
    ISNULL(iws.OnHand, 0)    AS OnHand,
    ISNULL(iws.Reserved, 0)  AS Reserved,
    ISNULL(iws.MinQty, 0)    AS MinQty,
    iws.ReorderQty           AS ReorderQty,
    iws.MaxQty               AS MaxQty,
    iws.LeadTimeDays         AS LeadDays,
    CAST(0 AS DECIMAL(18,3)) AS UsageHorizon,
    ISNULL(iws.MinQty, 0)    AS SafetyStock,

    sp.Id                    AS SupplierId,
    sp.Name                  AS SupplierName,
    ISNULL(ip.Price, 0)      AS Price,
    ISNULL(ip.Qty, 0)        AS Qty,
    ip.Barcode               AS Barcode
FROM dbo.ItemWarehouseStock iws
JOIN dbo.ItemMaster  im ON im.Id = iws.ItemId
JOIN dbo.Warehouse   w  ON w.Id  = iws.WarehouseId
LEFT JOIN ipx ip
       ON ip.ItemId      = im.Id
      AND ip.WarehouseId = iws.WarehouseId
      AND ip.rn = 1
LEFT JOIN dbo.Suppliers sp ON sp.Id = ip.SupplierId
WHERE iws.WarehouseId = @WarehouseId
ORDER BY im.Sku, im.Name, sp.Name;";

            var args = new { WarehouseId = warehouseId };

            if (Connection.State != ConnectionState.Open)
                await ((SqlConnection)Connection).OpenAsync();

            var map = new Dictionary<long, StockReorderWarehouseItems>();

            await Connection.QueryAsync<StockReorderWarehouseItems, StockReorderSupplier, StockReorderWarehouseItems>(
                sql,
                (item, sup) =>
                {
                    if (!map.TryGetValue(item.ItemId, out var existing))
                    {
                        existing = item;
                        existing.Suppliers = new List<StockReorderSupplier>();
                        map[item.ItemId] = existing;
                    }

                    // add only when a warehouse-specific ItemPrice row exists
                    if (sup != null && sup.SupplierId > 0)
                        existing.Suppliers.Add(sup);

                    return existing;
                },
                args,
                splitOn: "SupplierId"
            );

            return map.Values;
        }

    }
}
