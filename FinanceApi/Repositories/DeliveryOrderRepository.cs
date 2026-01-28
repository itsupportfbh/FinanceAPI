using System.Data;
using System.Text.RegularExpressions;
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using static FinanceApi.ModelDTO.DeliveryOrderDtos;

namespace FinanceApi.Repositories
{
    public class DeliveryOrderRepository : DynamicRepository, IDeliveryOrderRepository
    {
        public DeliveryOrderRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory) { }

        // ======== local types ========

        private sealed class SoLineIdsRow
        {
            public int Id { get; set; }
            public int? ItemId { get; set; }
            public string? WarehouseId { get; set; }
            public string? BinId { get; set; }
            public string? SupplierId { get; set; }
        }

        private sealed class SoLineMini
        {
            public int? ItemId { get; set; }
            public string? WarehouseId { get; set; }
            public string? BinId { get; set; }
            public string? SupplierId { get; set; }
        }

        private sealed class DoLineBasic
        {
            public int DoId { get; set; }
            public int? SoLineId { get; set; }
            public int? ItemId { get; set; }
            public int? WarehouseId { get; set; }
            public int? BinId { get; set; }
            public int? SupplierId { get; set; }
            public decimal Qty { get; set; }
        }

        private sealed class SoLineDeliverySnapshot
        {
            public int SoLineId { get; set; }
            public decimal Ordered { get; set; }
            public decimal DeliveredBefore { get; set; }
            public decimal DeliveredTotal { get; set; }
            public decimal PendingBefore { get; set; }
            public decimal PendingAfter { get; set; }
        }

        // ======== helpers ========

        /// Accepts int/string/"1,2,3"/dynamic and returns the FIRST parsable int, else null.
        private static int? NormalizeId(object? value)
        {
            if (value is null) return null;
            if (value is int i) return i;

            var s = value.ToString()?.Trim();
            if (string.IsNullOrWhiteSpace(s)) return null;

            foreach (var token in s.Split(new[] { ',', ';', '|', ' ' }, StringSplitOptions.RemoveEmptyEntries))
            {
                if (int.TryParse(token.Trim(), out var n)) return n;
            }
            return null;
        }

        private const string InsDol = @"
INSERT INTO dbo.DeliveryOrderLine
(DoId, SoLineId, PackLineId, ItemId, ItemName, Uom, Qty, Notes, WarehouseId, BinId, SupplierId)
OUTPUT INSERTED.Id
VALUES
(@DoId, @SoLineId, @PackLineId, @ItemId, @ItemName, @Uom, @Qty, @Notes, @WarehouseId, @BinId, @SupplierId);";

        // ---------- LockedQty helpers ----------
        private Task DecreaseLockedQtyAsync(int soLineId, decimal qty)
            => Connection.ExecuteAsync(@"
UPDATE dbo.SalesOrderLines
SET LockedQty = CASE 
                  WHEN ISNULL(LockedQty,0) - @Qty >= 0 THEN ISNULL(LockedQty,0) - @Qty
                  ELSE 0
                END,
    UpdatedDate = SYSUTCDATETIME()
WHERE Id=@Id;", new { Id = soLineId, Qty = qty });

        private Task IncreaseLockedQtyAsync(int soLineId, decimal qty)
            => Connection.ExecuteAsync(@"
UPDATE dbo.SalesOrderLines
SET LockedQty = ISNULL(LockedQty,0) + @Qty,
    UpdatedDate = SYSUTCDATETIME()
WHERE Id=@Id;", new { Id = soLineId, Qty = qty });

        /* ===================== Stock & Supplier qty adjust (NO TX) ===================== */
        private async Task AdjustStockAndPriceAsync(
            int? itemId, int? warehouseId, int? binId, int? supplierId,
            decimal qtyDelta)
        {
            if (itemId is null || warehouseId is null) return;

            // ItemWarehouseStock
            var upd = await Connection.ExecuteAsync(@"
UPDATE dbo.ItemWarehouseStock
SET
  OnHand    = CASE 
                WHEN @QtyDelta >= 0 
                     THEN CASE WHEN OnHand >= @QtyDelta THEN OnHand - @QtyDelta ELSE 0 END
                ELSE OnHand - @QtyDelta
              END,
  Reserved  = CASE 
                WHEN @QtyDelta >= 0 THEN CASE WHEN Reserved >= @QtyDelta THEN Reserved - @QtyDelta ELSE 0 END
                ELSE Reserved
              END,
  Available = (CASE 
                WHEN @QtyDelta >= 0 THEN CASE WHEN OnHand >= @QtyDelta THEN OnHand - @QtyDelta ELSE 0 END
                ELSE OnHand - @QtyDelta
              END) - 
              (CASE 
                WHEN @QtyDelta >= 0 THEN CASE WHEN Reserved >= @QtyDelta THEN Reserved - @QtyDelta ELSE 0 END
                ELSE Reserved
              END)
WHERE ItemId=@ItemId AND WarehouseId=@WarehouseId AND ISNULL(BinId,0)=ISNULL(@BinId,0);",
                new { ItemId = itemId.Value, WarehouseId = warehouseId.Value, BinId = (object?)binId ?? DBNull.Value, QtyDelta = qtyDelta });

            if (upd == 0)
            {
                await Connection.ExecuteAsync(@"
INSERT INTO dbo.ItemWarehouseStock
(ItemId, WarehouseId, BinId, StrategyId, OnHand, Reserved, MinQty, MaxQty, ReorderQty, LeadTimeDays,
 BatchFlag, SerialFlag, Available, IsTransfered, IsApproved, StockIssueID, IsFullTransfer, IsPartialTransfer)
VALUES
(@ItemId, @WarehouseId, @BinId, NULL, 0, 0, NULL, NULL, NULL, NULL, 0, 0, 0, 0, 0, NULL, 0, 0);",
                    new { ItemId = itemId.Value, WarehouseId = warehouseId.Value, BinId = (object?)binId ?? DBNull.Value });

                await Connection.ExecuteAsync(@"
UPDATE dbo.ItemWarehouseStock
SET
  OnHand    = CASE 
                WHEN @QtyDelta >= 0 THEN CASE WHEN OnHand >= @QtyDelta THEN OnHand - @QtyDelta ELSE 0 END
                ELSE OnHand - @QtyDelta
              END,
  Reserved  = CASE 
                WHEN @QtyDelta >= 0 THEN CASE WHEN Reserved >= @QtyDelta THEN Reserved - @QtyDelta ELSE 0 END
                ELSE Reserved
              END,
  Available = (CASE 
                WHEN @QtyDelta >= 0 THEN CASE WHEN OnHand >= @QtyDelta THEN OnHand - @QtyDelta ELSE 0 END
                ELSE OnHand - @QtyDelta
              END) -
              (CASE 
                WHEN @QtyDelta >= 0 THEN CASE WHEN Reserved >= @QtyDelta THEN Reserved - @QtyDelta ELSE 0 END
                ELSE Reserved
              END)
WHERE ItemId=@ItemId AND WarehouseId=@WarehouseId AND ISNULL(BinId,0)=ISNULL(@BinId,0);",
                    new { ItemId = itemId.Value, WarehouseId = warehouseId.Value, BinId = (object?)binId ?? DBNull.Value, QtyDelta = qtyDelta });
            }

            // ItemPrice
            var upd2 = await Connection.ExecuteAsync(@"
UPDATE dbo.ItemPrice
SET Qty = CASE 
            WHEN ISNULL(Qty,0) - @QtyDelta >= 0 THEN Qty - @QtyDelta 
            ELSE 0 
          END
WHERE ItemId=@ItemId AND WarehouseId=@WarehouseId AND ISNULL(SupplierId,0)=ISNULL(@SupplierId,0);",
                new
                {
                    ItemId = itemId.Value,
                    WarehouseId = warehouseId.Value,
                    SupplierId = (object?)supplierId ?? DBNull.Value,
                    QtyDelta = qtyDelta
                });

            if (upd2 == 0)
            {
                var price = 0m;   // safe default when inserting a new price row
                await Connection.ExecuteAsync(@"
INSERT INTO dbo.ItemPrice
(ItemId, SupplierId, Price, Barcode, Qty, WarehouseId, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate)
VALUES
(@ItemId, @SupplierId, @Price, @Barcode, 0, @WarehouseId, NULL, SYSUTCDATETIME(), NULL, NULL);",
                    new
                    {
                        ItemId = itemId.Value,
                        SupplierId = (object?)supplierId ?? DBNull.Value,
                        Price = price,
                        Barcode = "",
                        WarehouseId = warehouseId.Value
                    });

                await Connection.ExecuteAsync(@"
UPDATE dbo.ItemPrice
SET Qty = CASE 
            WHEN ISNULL(Qty,0) - @QtyDelta >= 0 THEN Qty - @QtyDelta 
            ELSE 0 
          END
WHERE ItemId=@ItemId AND WarehouseId=@WarehouseId AND ISNULL(SupplierId,0)=ISNULL(@SupplierId,0);",
                    new
                    {
                        ItemId = itemId.Value,
                        WarehouseId = warehouseId.Value,
                        SupplierId = (object?)supplierId ?? DBNull.Value,
                        QtyDelta = qtyDelta
                    });
            }
        }

        // =============== CREATE (NO TX) ===============
        public async Task<int> CreateAsync(DoCreateRequest req, int userId)
        {
            if (req.DriverId == 0) throw new ArgumentException("DriverId is required.");
            await EnsureOpenAsync().ConfigureAwait(false);

            // Prefetch context for SO lines (string IDs -> int via NormalizeId)
            var soLineIds = (req.Lines ?? new())
                .Where(x => x.SoLineId.HasValue)
                .Select(x => x.SoLineId!.Value)
                .Distinct()
                .ToArray();

            var soLineMap = soLineIds.Length == 0
                ? new Dictionary<int, (int? ItemId, int? Wh, int? Bin, int? Sup)>()
                : (await Connection.QueryAsync<SoLineIdsRow>(@"
                    SELECT 
                        l.Id,
                        l.ItemId,
                        CAST(l.WarehouseId AS nvarchar(50)) AS WarehouseId,
                        CAST(l.BinId       AS nvarchar(50)) AS BinId,
                        CAST(l.SupplierId  AS nvarchar(50)) AS SupplierId
                    FROM dbo.SalesOrderLines l
                    WHERE l.Id IN @ids;", new { ids = soLineIds }))
                  .ToDictionary(
                      r => r.Id,
                      r => (r.ItemId,
                            NormalizeId(r.WarehouseId),
                            NormalizeId(r.BinId),
                            NormalizeId(r.SupplierId))
                  );

            // Precompute IsPosted
            var thisDoPairs = (req.Lines ?? new())
                .Where(l => l.SoLineId.HasValue && l.Qty > 0)
                .Select(l => (SoLineId: l.SoLineId!.Value, Qty: l.Qty))
                .ToList();

            bool isPosted = false;
            if (req.SoId.HasValue && thisDoPairs.Count > 0)
            {
                var ids = thisDoPairs.Select(p => p.SoLineId).Distinct().ToArray();
                var snap = await GetSoLineSnapshotIncludingThisDoAsync(ids, thisDoPairs, excludeDoId: null);
                isPosted = snap.Values.All(s => DecimalsEqual(s.PendingAfter, 0m));
            }

            // Header
            string doNumber = await GenerateNextDoNumberWithRetryAsync().ConfigureAwait(false);
            var doId = await Connection.QuerySingleAsync<int>(@"
INSERT INTO dbo.DeliveryOrder
(DoNumber, Status, SoId, PackId, DriverId, VehicleId, RouteName, DeliveryDate,
 PodFileUrl, IsPosted, CreatedBy, CreatedOn,DriverMobileNo, ReceivedPersonName, ReceivedPersonMobileNo)
OUTPUT INSERTED.Id
VALUES
(@DoNumber, 0, @SoId, @PackId, @DriverId, @VehicleId, @RouteName, @DeliveryDate,
 NULL, @IsPosted, @UserId, SYSUTCDATETIME(),@DriverMobileNo, @ReceivedPersonName, @ReceivedPersonMobileNo);",
                new
                {
                    DoNumber = doNumber,
                    req.SoId,
                    req.PackId,
                    req.DriverId,
                    req.VehicleId,
                    req.RouteName,
                    req.DeliveryDate,
                     req.DriverMobileNo,
                    req.ReceivedPersonName,
                   req.ReceivedPersonMobileNo,

                    IsPosted = isPosted ? 1 : 0,
                    UserId = userId
                }).ConfigureAwait(false);

            // Lines
            foreach (var l in req.Lines ?? Enumerable.Empty<DoCreateRequest.DoCreateLine>())
            {
                int? itemId = NormalizeId(l.ItemId);
                int? wh = NormalizeId(l.WarehouseId);
                int? bin = NormalizeId(l.BinId);
                int? sup = NormalizeId(l.SupplierId);

                if (l.SoLineId.HasValue && soLineMap.TryGetValue(l.SoLineId.Value, out var s))
                {
                    itemId ??= s.Item1;
                    wh ??= s.Item2;
                    bin ??= s.Item3;
                    sup ??= s.Item4;
                }

                var lineId = await Connection.QuerySingleAsync<int>(InsDol, new
                {
                    DoId = doId,
                    l.SoLineId,
                    l.PackLineId,
                    ItemId = itemId,
                    ItemName = l.ItemName ?? string.Empty,
                    l.Uom,
                    l.Qty,
                    l.Notes,
                    WarehouseId = wh,
                    BinId = bin,
                    SupplierId = sup
                }).ConfigureAwait(false);

                await AdjustStockAndPriceAsync(itemId, wh, bin, sup, l.Qty).ConfigureAwait(false);

                if (l.SoLineId.HasValue)
                    await DecreaseLockedQtyAsync(l.SoLineId.Value, l.Qty).ConfigureAwait(false);

                if (req.SoId.HasValue && isPosted)
                {
                    await Connection.ExecuteAsync(@"
UPDATE dbo.SalesOrder
SET Status = 3,
    UpdatedBy = @UserId,
    UpdatedDate = SYSUTCDATETIME()
WHERE Id = @SoId;",
                        new { SoId = req.SoId.Value, UserId = userId }).ConfigureAwait(false);
                }

            }

            return doId;
        }

        // =============== READ ===============
        public Task<DoHeaderEditDto?> GetHeaderAsync(int id)
    => Connection.QuerySingleOrDefaultAsync<DoHeaderEditDto>(@"
SELECT
  DO.Id,
  DO.DoNumber,
  DO.Status,
  DO.SoId,
  DO.PackId,

  CAST(NULL AS nvarchar(50)) AS InvoiceNo,
  CAST(NULL AS int)          AS SiId,

  DO.DriverId,
  DO.VehicleId,
  DO.RouteName,
  DO.DeliveryDate,
  DO.PodFileUrl,
  DO.IsPosted,

  -- ✅ NEW FIELDS
  DO.DriverMobileNo,
  DO.ReceivedPersonName,
  DO.ReceivedPersonMobileNo,

  SO.SalesOrderNo,

  SO.CustomerId,
  C.CustomerName AS CustomerName
FROM dbo.DeliveryOrder DO
LEFT JOIN dbo.SalesOrder SO ON SO.Id = DO.SoId
LEFT JOIN dbo.Customer C ON C.Id = SO.CustomerId
WHERE DO.Id = @id;
", new { id });



        public Task<IEnumerable<DoLineDto>> GetLinesAsync(int doId)
            => Connection.QueryAsync<DoLineDto>(@"
SELECT 
    Id, DoId, SoLineId, PackLineId, ItemId, ItemName, Uom, Qty, Notes,
    CAST(WarehouseId AS nvarchar(50)) AS WarehouseId,
    CAST(BinId       AS nvarchar(50)) AS BinId,
    CAST(SupplierId  AS nvarchar(50)) AS SupplierId
FROM dbo.DeliveryOrderLine
WHERE DoId=@doId;", new { doId });

        public Task<IEnumerable<DoHeaderDto>> GetAllAsync()
   => Connection.QueryAsync<DoHeaderDto>(@"
SELECT 
    d.Id,
    d.DoNumber,
    d.Status,
    d.SoId,
    d.PackId,
    si1.InvoiceNo,
    si1.Id       AS SiId,
    d.DriverId,
    d.VehicleId,
    d.RouteName,
    d.DeliveryDate,
    d.PodFileUrl,
    d.IsPosted,

    -- ✅ NEW
    d.DriverMobileNo,
    d.ReceivedPersonName,
    d.ReceivedPersonMobileNo,

    s.SalesOrderNo,
    s.CustomerId,
    c.CustomerName
FROM dbo.DeliveryOrder d
LEFT JOIN dbo.SalesOrder s 
    ON s.Id = d.SoId
OUTER APPLY (
    SELECT TOP (1) si.*
    FROM dbo.SalesInvoice si
    WHERE si.DoId = d.Id
    ORDER BY si.Id DESC
) si1
LEFT JOIN dbo.Customer c 
    ON c.Id = s.CustomerId
ORDER BY d.Id DESC;
");


        // =============== UPDATE HEADER ===============
        public async Task UpdateHeaderAsync(int id, DoUpdateHeaderRequest req, int userId)
        {
            await EnsureOpenAsync().ConfigureAwait(false);
            await Connection.ExecuteAsync(@"
UPDATE dbo.DeliveryOrder SET
  DriverId=@DriverId,
  VehicleId=@VehicleId,
  RouteName=@RouteName,
  DeliveryDate=@DeliveryDate,
  UpdatedBy=@UserId,
  UpdatedOn=SYSUTCDATETIME()
WHERE Id=@Id;",
                new { Id = id, req.DriverId, req.VehicleId, req.RouteName, req.DeliveryDate, UserId = userId }).ConfigureAwait(false);

            await RecalcAndPersistIsPostedAsync(id).ConfigureAwait(false);
        }

        // =============== ADD LINE (NO TX) ===============
        public async Task<int> AddLineAsync(DoAddLineRequest req, int userId)
        {
            await EnsureOpenAsync().ConfigureAwait(false);

            int? itemId = NormalizeId(req.ItemId);
            int? wh = NormalizeId(req.WarehouseId);
            int? bin = NormalizeId(req.BinId);
            int? sup = NormalizeId(req.SupplierId);

            if (req.SoLineId.HasValue)
            {
                var s = await Connection.QuerySingleOrDefaultAsync<SoLineMini>(@"
SELECT 
    ItemId,
    CAST(WarehouseId AS nvarchar(50)) AS WarehouseId,
    CAST(BinId       AS nvarchar(50)) AS BinId,
    CAST(SupplierId  AS nvarchar(50)) AS SupplierId
FROM dbo.SalesOrderLines WHERE Id=@id;", new { id = req.SoLineId.Value }).ConfigureAwait(false);

                if (s != null)
                {
                    itemId ??= s.ItemId;
                    wh ??= NormalizeId(s.WarehouseId);
                    bin ??= NormalizeId(s.BinId);
                    sup ??= NormalizeId(s.SupplierId);
                }
            }

            var lineId = await Connection.QuerySingleAsync<int>(InsDol, new
            {
                req.DoId,
                req.SoLineId,
                req.PackLineId,
                ItemId = itemId,
                req.ItemName,
                req.Uom,
                req.Qty,
                req.Notes,
                WarehouseId = wh,
                BinId = bin,
                SupplierId = sup
            }).ConfigureAwait(false);

            await AdjustStockAndPriceAsync(itemId, wh, bin, sup, req.Qty).ConfigureAwait(false);

            if (req.SoLineId.HasValue)
                await DecreaseLockedQtyAsync(req.SoLineId.Value, req.Qty).ConfigureAwait(false);

            await RecalcAndPersistIsPostedAsync(req.DoId).ConfigureAwait(false);
            return lineId;
        }

        // =============== REMOVE LINE (NO TX) ===============
        public async Task RemoveLineAsync(int lineId)
        {
            await EnsureOpenAsync().ConfigureAwait(false);

            var line = await Connection.QuerySingleOrDefaultAsync<DoLineBasic>(@"
SELECT DoId, SoLineId, ItemId, WarehouseId, BinId, SupplierId, Qty
FROM dbo.DeliveryOrderLine WHERE Id=@lineId;", new { lineId }).ConfigureAwait(false);

            if (line == null) return;

            int doId = line.DoId;
            int? itemId = line.ItemId;
            int? wh = line.WarehouseId;
            int? bin = line.BinId;
            int? sup = line.SupplierId;
            decimal qty = line.Qty;
            int? soLineId = line.SoLineId;

            await Connection.ExecuteAsync("DELETE FROM dbo.DeliveryOrderLine WHERE Id=@lineId;", new { lineId }).ConfigureAwait(false);
            await AdjustStockAndPriceAsync(itemId, wh, bin, sup, -qty).ConfigureAwait(false);

            if (soLineId.HasValue)
                await IncreaseLockedQtyAsync(soLineId.Value, qty).ConfigureAwait(false);

            if (doId > 0) await RecalcAndPersistIsPostedAsync(doId).ConfigureAwait(false);
        }

        // =============== STATUS ===============
        public Task SetStatusAsync(int id, int status, int userId)
            => Connection.ExecuteAsync(@"
UPDATE dbo.DeliveryOrder
SET Status=@status, UpdatedBy=@userId, UpdatedOn=SYSUTCDATETIME()
WHERE Id=@id;", new { id, status, userId });

        public Task PostAsync(int id, int userId)
            => Connection.ExecuteAsync(@"
UPDATE dbo.DeliveryOrder
SET Status=4, IsPosted=1, UpdatedBy=@userId, UpdatedOn=SYSUTCDATETIME()
WHERE Id=@id;", new { id, userId });

        // =============== internals ===============
        private async Task<string> GenerateNextDoNumberWithRetryAsync(int maxRetries = 5)
        {
            int attempt = 0;
            while (true)
            {
                attempt++;
                var last = await Connection.QueryFirstOrDefaultAsync<string>(@"
SELECT TOP (1) DoNumber
FROM dbo.DeliveryOrder WITH (READPAST)
WHERE DoNumber LIKE 'DO-%'
ORDER BY Id DESC;").ConfigureAwait(false);
                int next = 1;
                if (!string.IsNullOrWhiteSpace(last))
                {
                    var m = Regex.Match(last, @"^DO-(\d+)$");
                    if (m.Success && int.TryParse(m.Groups[1].Value, out var n))
                        next = n + 1;
                }
                var candidate = $"DO-{next:D6}";

                var exists = await Connection.ExecuteScalarAsync<int>(
                    "SELECT COUNT(1) FROM dbo.DeliveryOrder WHERE DoNumber=@n;", new { n = candidate }).ConfigureAwait(false);
                if (exists == 0 || attempt >= maxRetries) return candidate;
            }
        }

        private static bool DecimalsEqual(decimal a, decimal b, decimal tol = 0.0001m)
            => Math.Abs(a - b) <= tol;

        private async Task<Dictionary<int, SoLineDeliverySnapshot>> GetSoLineSnapshotIncludingThisDoAsync(
            int[] soLineIds, IEnumerable<(int SoLineId, decimal Qty)> thisDoLines, int? excludeDoId)
        {
            const string sql = @"
SELECT
    l.Id AS SoLineId,
    CAST(l.Quantity AS decimal(18,4)) AS Ordered,
    CAST(ISNULL((SELECT SUM(dol.Qty)
                 FROM dbo.DeliveryOrderLine dol
                 WHERE dol.SoLineId = l.Id
                   AND (@excludeDoId IS NULL OR dol.DoId <> @excludeDoId)
    ), 0) AS decimal(18,4)) AS DeliveredBefore
FROM dbo.SalesOrderLines l
WHERE l.Id IN @ids;";

            var baseRows = await Connection.QueryAsync<(int SoLineId, decimal Ordered, decimal DeliveredBefore)>(
                sql, new { ids = soLineIds, excludeDoId }).ConfigureAwait(false);

            var thisDoMap = thisDoLines.GroupBy(x => x.SoLineId).ToDictionary(g => g.Key, g => g.Sum(v => v.Qty));

            var dict = new Dictionary<int, SoLineDeliverySnapshot>(soLineIds.Length);
            foreach (var r in baseRows)
            {
                var thisQty = thisDoMap.TryGetValue(r.SoLineId, out var q) ? q : 0m;
                var deliveredTotal = r.DeliveredBefore + thisQty;
                var pendingBefore = Math.Max(r.Ordered - r.DeliveredBefore, 0m);
                var pendingAfter = Math.Max(r.Ordered - deliveredTotal, 0m);

                dict[r.SoLineId] = new SoLineDeliverySnapshot
                {
                    SoLineId = r.SoLineId,
                    Ordered = r.Ordered,
                    DeliveredBefore = r.DeliveredBefore,
                    DeliveredTotal = deliveredTotal,
                    PendingBefore = pendingBefore,
                    PendingAfter = pendingAfter
                };
            }
            return dict;
        }

        private async Task RecalcAndPersistIsPostedAsync(int doId)
        {
            var soId = await Connection.QuerySingleOrDefaultAsync<int?>(
                "SELECT SoId FROM dbo.DeliveryOrder WHERE Id=@doId;", new { doId }).ConfigureAwait(false);

            var lines = (await Connection.QueryAsync<(int? SoLineId, decimal Qty)>(
                "SELECT SoLineId, Qty FROM dbo.DeliveryOrderLine WHERE DoId=@doId;", new { doId }).ConfigureAwait(false)).ToList();

            bool isPosted = false;

            if (soId.HasValue && lines.Count > 0 && lines.All(x => x.SoLineId.HasValue && x.Qty > 0))
            {
                var grouped = lines.GroupBy(x => x.SoLineId!.Value).ToDictionary(g => g.Key, g => g.Sum(v => v.Qty));
                var ids = grouped.Keys.ToArray();
                var snap = await GetSoLineSnapshotIncludingThisDoAsync(ids, grouped.Select(kvp => (kvp.Key, kvp.Value)), excludeDoId: doId).ConfigureAwait(false);
                isPosted = snap.Values.All(s => DecimalsEqual(s.PendingAfter, 0m));
            }

            await Connection.ExecuteAsync(@"
UPDATE dbo.DeliveryOrder
SET IsPosted=@isPosted, UpdatedOn=SYSUTCDATETIME()
WHERE Id=@doId;", new { doId, isPosted }).ConfigureAwait(false);
        }

        private async Task EnsureOpenAsync()
        {
            if (Connection.State != ConnectionState.Open)
            {
                if (Connection is System.Data.Common.DbConnection dbc)
                    await dbc.OpenAsync().ConfigureAwait(false);
                else
                    Connection.Open();
            }
        }

        // =============== Re-delivery view ===============
        public async Task<IEnumerable<object>> GetSoRedeliveryViewAsync(int doId, int soId)
        {
            // Sales order lines (now includes WH/Bin/Supplier as strings)
            var soLines = await Connection.QueryAsync<(
                int SoLineId, int? ItemId, string ItemName, string Uom, decimal Ordered,
                string WarehouseId, string BinId, string SupplierId
            )>(@"
SELECT 
    l.Id AS SoLineId,
    l.ItemId,
    l.ItemName,
    l.Uom,
    CAST(l.Quantity AS decimal(18,4)) AS Ordered,
    CAST(l.WarehouseId AS nvarchar(200)) AS WarehouseId,
    CAST(l.BinId       AS nvarchar(200)) AS BinId,
    CAST(l.SupplierId  AS nvarchar(200)) AS SupplierId
FROM dbo.SalesOrderLines l
WHERE l.SalesOrderId=@soId;", new { soId });

            // Delivered on other DOs
            var other = await Connection.QueryAsync<(int SoLineId, decimal Qty)>(@"
SELECT SoLineId, SUM(Qty) AS Qty
FROM dbo.DeliveryOrderLine
WHERE SoLineId IS NOT NULL AND DoId<>@doId
GROUP BY SoLineId;", new { doId });
            var otherMap = other.ToDictionary(x => x.SoLineId, x => x.Qty);

            // Delivered on this DO
            var thisDo = await Connection.QueryAsync<(int SoLineId, decimal Qty)>(@"
SELECT SoLineId, SUM(Qty) AS Qty
FROM dbo.DeliveryOrderLine
WHERE DoId=@doId AND SoLineId IS NOT NULL
GROUP BY SoLineId;", new { doId });
            var thisMap = thisDo.ToDictionary(x => x.SoLineId, x => x.Qty);

            var list = new List<object>();
            foreach (var s in soLines)
            {
                var before = otherMap.TryGetValue(s.SoLineId, out var ob) ? ob : 0m;
                var onThis = thisMap.TryGetValue(s.SoLineId, out var tb) ? tb : 0m;
                var pending = Math.Max(s.Ordered - (before + onThis), 0m);

                list.Add(new
                {
                    soLineId = s.SoLineId,
                    itemId = s.ItemId,
                    itemName = s.ItemName,
                    uom = s.Uom,
                    ordered = s.Ordered,
                    deliveredBefore = before,
                    deliveredOnThisDo = onThis,
                    pending = pending,
                    // NEW: pass through from SalesOrderLines
                    warehouseId = s.WarehouseId,   // string (may contain "1,2")
                    binId = s.BinId,               // string (may contain "10|11")
                    supplierId = s.SupplierId      // string (may contain "3")
                });
            }
            return list;
        }

    }
}
