// Repositories/SalesOrderRepository.cs
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Text.Json;
using static FinanceApi.ModelDTO.AllocationPreviewRequest;
using static FinanceApi.ModelDTO.QutationDetailsViewInfo;

namespace FinanceApi.Repositories
{
    public class SalesOrderRepository : DynamicRepository, ISalesOrderRepository
    {
        public SalesOrderRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory) { }

        // ====================================================
        // ===================== READ LIST ====================
        // ====================================================
        public async Task<IEnumerable<SalesOrderDTO>> GetAllAsync()
        {
            const string headersSql = @"
SELECT
    so.Id,
    so.QuotationNo,
    so.CustomerId,
    ISNULL(c.CustomerName,'') AS CustomerName,
    so.RequestedDate,
    so.DeliveryDate,

    so.DeliveryTo,
    so.Remarks,

    so.Status,
    so.Shipping,
    so.Discount,
    so.GstPct,
    so.CreatedBy,
    so.CreatedDate,
    so.UpdatedBy,
    so.UpdatedDate,
    so.IsActive,
    so.SalesOrderNo,
    ISNULL(so.Subtotal,0)    AS Subtotal,
    ISNULL(so.TaxAmount,0)   AS TaxAmount,
    ISNULL(so.GrandTotal,0)  AS GrandTotal,
    so.ApprovedBy
FROM dbo.SalesOrder so
LEFT JOIN dbo.Customer c ON c.Id = so.CustomerId
WHERE so.IsActive = 1
ORDER BY so.Id;";

            var headers = (await Connection.QueryAsync<SalesOrderDTO>(headersSql)).ToList();
            if (headers.Count == 0) return headers;

            var ids = headers.Select(h => h.Id).ToArray();

            const string linesSql = @"
SELECT
    Id,
    SalesOrderId,
    ItemId,
    ItemName,
    Uom,
    [Description],
    Quantity,
    UnitPrice,
    Discount,
    Tax,
    TaxCodeId,
    TaxAmount,
    Total,
    WarehouseId,
    BinId,
    Available,
    SupplierId,
    LockedQty,
    CreatedBy,
    CreatedDate,
    UpdatedBy,
    UpdatedDate,
    IsActive
FROM dbo.SalesOrderLines
WHERE SalesOrderId IN @Ids AND IsActive = 1;";

            var lines = await Connection.QueryAsync<SalesOrderLineDTO>(linesSql, new { Ids = ids });

            var map = headers.ToDictionary(h => h.Id);
            foreach (var ln in lines)
                if (map.TryGetValue(ln.SalesOrderId, out var parent))
                    parent.LineItems.Add(ln);

            return headers;
        }

        // ====================================================
        // ===================== READ ONE =====================
        // ====================================================
        public async Task<SalesOrderDTO?> GetByIdAsync(int id)
        {
            const string headerSql = @"
SELECT TOP(1)
    so.Id,
    so.QuotationNo,
    q.Number,
    so.CustomerId,
    ISNULL(c.CustomerName,'') AS CustomerName,
    so.RequestedDate,
    so.DeliveryDate,

    so.DeliveryTo,
    so.Remarks,

    so.Status,
    so.Shipping,
    so.Discount,
    so.GstPct,
    so.CreatedBy,
    so.CreatedDate,
    so.UpdatedBy,
    so.UpdatedDate,
    so.IsActive,
    so.SalesOrderNo,
    ISNULL(so.Subtotal,0)    AS Subtotal,
    ISNULL(so.TaxAmount,0)   AS TaxAmount,
    ISNULL(so.GrandTotal,0)  AS GrandTotal,
    so.ApprovedBy,
so.DeliveryTo
FROM dbo.SalesOrder so
LEFT JOIN dbo.Customer  c ON c.Id = so.CustomerId
LEFT JOIN dbo.Quotation q ON q.Id = so.QuotationNo
WHERE so.Id = @Id AND so.IsActive = 1;";

            var head = await Connection.QueryFirstOrDefaultAsync<SalesOrderDTO>(headerSql, new { Id = id });
            if (head is null) return null;

            const string linesSql = @"
SELECT
    sl.Id,
    sl.SalesOrderId,
    sl.ItemId,
    sl.ItemName,
    sl.Uom,
    sl.[Description],
    sl.Quantity,
    sl.UnitPrice,
    sl.Discount,
    sl.Tax,
    sl.TaxCodeId,
    sl.TaxAmount,
    sl.Total,
    sl.WarehouseId,
    ISNULL(w.Name,'')     AS WarehouseName,
    sl.SupplierId,
    ISNULL(s.Name,'')     AS SupplierName,
    sl.BinId,
    ISNULL(b.BinName,'')  AS Bin,
    sl.Available,
    sl.LockedQty,
    sl.CreatedBy,
    sl.CreatedDate,
    sl.UpdatedBy,
    sl.UpdatedDate,
    sl.IsActive
FROM dbo.SalesOrderLines sl
LEFT JOIN dbo.Warehouse w ON w.Id = sl.WarehouseId
LEFT JOIN dbo.Suppliers s ON s.Id = sl.SupplierId
LEFT JOIN dbo.Bin       b ON b.Id = sl.BinId
WHERE sl.SalesOrderId = @Id
  AND sl.IsActive = 1
ORDER BY sl.Id;";

            var lines = await Connection.QueryAsync<SalesOrderLineDTO>(linesSql, new { Id = id });
            head.LineItems = lines.ToList();
            return head;
        }

        // ====================================================
        // ============= ALLOCATION HELPERS ===================
        // ====================================================
        private readonly record struct AllocCandidate(int WarehouseId, int? BinId, int SupplierId, decimal WhAvail, decimal SupplierQty);
        private readonly record struct Allocation(int WarehouseId, int? BinId, int SupplierId, decimal Qty);

        private async Task EnsureOpenAsync(IDbConnection conn)
        {
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();
        }

        private async Task<int?> GetItemMasterIdAsync(IDbConnection conn, IDbTransaction tx, int itemId)
        {
            const string sql = @"
SELECT TOP 1 im.Id
FROM dbo.Item i WITH (NOLOCK)
JOIN dbo.ItemMaster im WITH (NOLOCK) ON im.Sku = i.ItemCode
WHERE i.Id = @ItemId;";
            return await conn.ExecuteScalarAsync<int?>(sql, new { ItemId = itemId }, tx);
        }

        private async Task<List<AllocCandidate>> GetAllocCandidatesAsync(IDbConnection conn, IDbTransaction tx, int itemMasterId)
        {
            const string sql = @"
WITH IWS AS (
    SELECT W.WarehouseId, W.BinId, W.ItemId,
           SUM(W.OnHand) AS OnHand, SUM(W.Reserved) AS Reserved
    FROM dbo.ItemWarehouseStock W WITH (NOLOCK)
    WHERE W.ItemId = @ItemMasterId
    GROUP BY W.WarehouseId, W.BinId, W.ItemId
),
LCK AS (
    SELECT sol.WarehouseId, SUM(sol.LockedQty) AS LckQty
    FROM dbo.SalesOrderLines sol WITH (NOLOCK)
    JOIN dbo.SalesOrder so  WITH (NOLOCK) ON so.Id = sol.SalesOrderId AND so.IsActive = 1
    JOIN dbo.Item i        WITH (NOLOCK) ON i.Id  = sol.ItemId
    JOIN dbo.ItemMaster im WITH (NOLOCK) ON im.Sku = i.ItemCode
    WHERE sol.IsActive = 1
      AND im.Id = @ItemMasterId
    GROUP BY sol.WarehouseId
)
SELECT 
    ip.WarehouseId,
    iws.BinId,
    ip.SupplierId,
    CASE 
      WHEN iws.ItemId IS NULL THEN 0
      ELSE CASE 
             WHEN (ISNULL(iws.OnHand,0) - ISNULL(iws.Reserved,0)) - ISNULL(lck.LckQty,0) < 0 
                  THEN 0 
             ELSE (ISNULL(iws.OnHand,0) - ISNULL(iws.Reserved,0)) - ISNULL(lck.LckQty,0)
           END
    END AS WhAvail,
    ip.Qty AS SupplierQty
FROM dbo.ItemPrice ip WITH (NOLOCK)
LEFT JOIN IWS iws ON iws.ItemId = ip.ItemId AND iws.WarehouseId = ip.WarehouseId
LEFT JOIN LCK lck ON lck.WarehouseId = ip.WarehouseId
WHERE ip.ItemId = @ItemMasterId
  AND ip.Qty > 0
ORDER BY WhAvail DESC, ip.Qty DESC;";
            var rows = await conn.QueryAsync<AllocCandidate>(sql, new { ItemMasterId = itemMasterId }, tx);
            return rows.ToList();
        }

        private static List<Allocation> MakeAllocation(List<AllocCandidate> cands, decimal requiredQty)
        {
            var left = requiredQty;
            var res = new List<Allocation>();
            foreach (var c in cands)
            {
                if (left <= 0) break;
                var take = Math.Min(left, Math.Min(c.WhAvail, c.SupplierQty));
                if (take <= 0) continue;
                res.Add(new Allocation(c.WarehouseId, c.BinId, c.SupplierId, take));
                left -= take;
            }
            return res;
        }

        private static decimal Round2(decimal v) =>
            Math.Round(v, 2, MidpointRounding.AwayFromZero);

        private static (decimal net, decimal taxAmt, decimal total, decimal discountValue) ComputeAmounts(
            decimal qty, decimal unitPrice, decimal discountPct, string? taxMode, decimal gstPct)
        {
            var sub = qty * unitPrice;

            var discountValue = Round2(sub * (discountPct / 100m));
            var afterDisc = sub - discountValue;
            if (afterDisc < 0) afterDisc = 0;

            var sMode = (taxMode ?? "EXEMPT").ToUpperInvariant();
            var rate = gstPct / 100m;

            decimal net, tax, tot;

            switch (sMode)
            {
                case "EXCLUSIVE":
                case "STANDARD-RATED":
                case "STANDARD_RATED":
                    net = afterDisc;
                    tax = Round2(net * rate);
                    tot = net + tax;
                    break;

                case "INCLUSIVE":
                    tot = afterDisc;
                    net = rate > 0 ? Round2(tot / (1 + rate)) : tot;
                    tax = tot - net;
                    break;

                default:
                    net = afterDisc;
                    tax = 0;
                    tot = afterDisc;
                    break;
            }

            return (net, tax, tot, discountValue);
        }

        private async Task<DateTime?> GetQuotationDeliveryDateAsync(IDbConnection conn, IDbTransaction tx, int quotationId)
        {
            const string sql = @"
SELECT TOP(1) DeliveryDate
FROM dbo.Quotation WITH (NOLOCK)
WHERE Id = @Id AND IsActive = 1;";
            return await conn.ExecuteScalarAsync<DateTime?>(sql, new { Id = quotationId }, tx);
        }

        // ======= NEW: insert ordered SO line (NO Qty=0 rows) =======
        private async Task<int> InsertSalesOrderLineAsync(
            IDbConnection conn, IDbTransaction tx,
            int salesOrderId, SalesOrderLines l, SalesOrder so, DateTime now)
        {
            var (net, tax, total, _) = ComputeAmounts(
                l.Quantity, l.UnitPrice, l.Discount, l.Tax ?? "EXEMPT", so.GstPct);

            const string sql = @"
INSERT INTO dbo.SalesOrderLines
(SalesOrderId, ItemId, ItemName, Uom, [Description],
 Quantity, UnitPrice, Discount, Tax, TaxCodeId, TaxAmount, Total,
 WarehouseId, BinId, Available, SupplierId, LockedQty,
 CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive)
OUTPUT INSERTED.Id
VALUES
(@SalesOrderId, @ItemId, @ItemName, @Uom, @Description,
 @Quantity, @UnitPrice, @Discount, @Tax, @TaxCodeId, @TaxAmount, @Total,
 NULL, NULL, 0, NULL, 0,
 @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1);";

            return await conn.ExecuteScalarAsync<int>(sql, new
            {
                SalesOrderId = salesOrderId,
                l.ItemId,
                l.ItemName,
                l.Uom,
                Description = (object?)l.Description ?? DBNull.Value,

                Quantity = l.Quantity,
                UnitPrice = l.UnitPrice,
                Discount = l.Discount,
                Tax = l.Tax,
                l.TaxCodeId,
                TaxAmount = tax,
                Total = total,

                CreatedBy = so.CreatedBy,
                CreatedDate = so.CreatedDate == default ? now : so.CreatedDate,
                UpdatedBy = so.UpdatedBy ?? so.CreatedBy,
                UpdatedDate = so.UpdatedDate ?? now
            }, tx);
        }

        // ======= NEW: insert allocations into SalesOrderLineAlloc =======
        private async Task<int> InsertAllocRowsAsync(
            IDbConnection conn, IDbTransaction tx,
            int salesOrderLineId, List<Allocation> allocs, SalesOrder so, DateTime now)
        {
            if (allocs == null || allocs.Count == 0) return 0;

            const string ins = @"
INSERT INTO dbo.SalesOrderLineAlloc
(SalesOrderLineId, WarehouseId, BinId, SupplierId, Qty, IsActive, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate)
VALUES
(@SalesOrderLineId, @WarehouseId, @BinId, @SupplierId, @Qty, 1, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate);";

            foreach (var a in allocs)
            {
                await conn.ExecuteAsync(ins, new
                {
                    SalesOrderLineId = salesOrderLineId,
                    WarehouseId = a.WarehouseId,
                    BinId = a.BinId,
                    SupplierId = a.SupplierId,
                    Qty = a.Qty,
                    CreatedBy = so.CreatedBy,
                    CreatedDate = so.CreatedDate == default ? now : so.CreatedDate,
                    UpdatedBy = so.UpdatedBy ?? so.CreatedBy,
                    UpdatedDate = so.UpdatedDate ?? now
                }, tx);
            }

            return (int)allocs.Sum(x => x.Qty);
        }

        // ======= Update line summary fields from allocations (first WH for display + LockedQty sum) =======
        private async Task UpdateLineAllocationSummaryAsync(
            IDbConnection conn, IDbTransaction tx,
            int salesOrderLineId)
        {
            const string sql = @"
;WITH A AS (
    SELECT TOP 1 WarehouseId, BinId, SupplierId
    FROM dbo.SalesOrderLineAlloc WITH (NOLOCK)
    WHERE SalesOrderLineId=@Id AND IsActive=1
    ORDER BY Qty DESC, Id ASC
),
S AS (
    SELECT ISNULL(SUM(Qty),0) AS AllocQty
    FROM dbo.SalesOrderLineAlloc WITH (NOLOCK)
    WHERE SalesOrderLineId=@Id AND IsActive=1
)
UPDATE L
SET L.WarehouseId = A.WarehouseId,
    L.BinId       = A.BinId,
    L.SupplierId  = A.SupplierId,
    L.LockedQty   = S.AllocQty
FROM dbo.SalesOrderLines L
CROSS JOIN S
LEFT JOIN A ON 1=1
WHERE L.Id=@Id;";
            await conn.ExecuteAsync(sql, new { Id = salesOrderLineId }, tx);
        }

        private async Task SoftDeleteAllocBySoAsync(IDbConnection conn, IDbTransaction tx, int? soId, int? updatedBy)
        {
            const string sql = @"
UPDATE a
SET IsActive=0, UpdatedBy=@UpdatedBy, UpdatedDate=SYSUTCDATETIME()
FROM dbo.SalesOrderLineAlloc a
JOIN dbo.SalesOrderLines l ON l.Id = a.SalesOrderLineId
WHERE l.SalesOrderId=@SoId AND a.IsActive=1;";
            await conn.ExecuteAsync(sql, new { SoId = soId, UpdatedBy = updatedBy }, tx);
        }

        private async Task RecomputeAndSetHeaderTotalsAsync(
            IDbConnection conn, IDbTransaction tx, int salesOrderId,
            decimal gstPct, decimal shipping, decimal headerExtraDiscount)
        {
            const string q = @"
SELECT 
    Quantity,
    UnitPrice,
    ISNULL(Discount,0)          AS DiscountPct,
    UPPER(ISNULL(Tax,'EXEMPT')) AS TaxMode
FROM dbo.SalesOrderLines WITH (NOLOCK)
WHERE SalesOrderId = @Id AND IsActive = 1;";

            var rows = await conn.QueryAsync<(decimal Quantity, decimal UnitPrice, decimal DiscountPct, string TaxMode)>(
                q, new { Id = salesOrderId }, tx);

            var rate = gstPct / 100m;

            decimal sumGross = 0m;
            decimal sumDiscVal = 0m;
            decimal sumTax = 0m;

            foreach (var row in rows)
            {
                var gross = row.Quantity * row.UnitPrice;
                var discVal = Round2(gross * (row.DiscountPct / 100m));
                var afterDisc = gross - discVal;
                if (afterDisc < 0) afterDisc = 0;

                decimal lineTax;
                var mode = (row.TaxMode ?? "EXEMPT").ToUpperInvariant();
                switch (mode)
                {
                    case "EXCLUSIVE":
                    case "STANDARD-RATED":
                    case "STANDARD_RATED":
                        lineTax = Round2(afterDisc * rate);
                        break;
                    case "INCLUSIVE":
                        if (rate > 0)
                        {
                            var net = Round2(afterDisc / (1 + rate));
                            lineTax = afterDisc - net;
                        }
                        else lineTax = 0;
                        break;
                    default:
                        lineTax = 0;
                        break;
                }

                sumGross += gross;
                sumDiscVal += discVal;
                sumTax += lineTax;
            }

            var extraHeaderDisc = headerExtraDiscount < 0 ? 0 : headerExtraDiscount;
            var totalDiscountVal = Round2(sumDiscVal + extraHeaderDisc);

            var subtotal = Round2(sumGross);
            var taxAmount = Round2(sumTax);
            var grand = Round2(subtotal - totalDiscountVal + taxAmount + shipping);

            const string upd = @"
UPDATE dbo.SalesOrder
SET Subtotal   = @Subtotal,
    TaxAmount  = @TaxAmount,
    GrandTotal = @GrandTotal,
    Discount   = @Discount,
    UpdatedDate = SYSUTCDATETIME()
WHERE Id=@Id;";

            await conn.ExecuteAsync(upd, new
            {
                Id = salesOrderId,
                Subtotal = subtotal,
                TaxAmount = taxAmount,
                GrandTotal = grand,
                Discount = totalDiscountVal
            }, tx);
        }

        // ===================== RUNNING NUMBER =====================
        private async Task<string> GetNextSalesOrderNoAsync(IDbConnection conn, IDbTransaction tx, string prefix = "SO-", int width = 4)
        {
            const string sql = @"
DECLARE @n INT;
SELECT @n = ISNULL(MAX(TRY_CONVERT(int, RIGHT(SalesOrderNo, @Width))), 0) + 1
FROM dbo.SalesOrder WITH (UPDLOCK, HOLDLOCK);
SELECT @n;";
            var next = await conn.ExecuteScalarAsync<int>(sql, new { Width = width }, transaction: tx);
            return $"{prefix}{next.ToString().PadLeft(width, '0')}";
        }

        // ====================================================
        // ===================== CREATE =======================
        // ====================================================
        public async Task<int> CreateAsync(SalesOrder so)
        {
            var now = DateTime.UtcNow;
            if (so.CreatedDate == default) so.CreatedDate = now;
            if (so.UpdatedDate == null) so.UpdatedDate = now;

            const string insertHeader = @"
INSERT INTO dbo.SalesOrder
(QuotationNo, CustomerId, RequestedDate, DeliveryDate,
 DeliveryTo, Remarks,
 Status, Shipping, Discount, GstPct,
 SalesOrderNo, SubTotal, TaxAmount, GrandTotal,
 CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive, ApprovedBy)
OUTPUT INSERTED.Id
VALUES
(@QuotationNo, @CustomerId, @RequestedDate, @DeliveryDate,
 @DeliveryTo, @Remarks,
 @Status, @Shipping, @Discount, @GstPct,
 @SalesOrderNo, @SubTotal, @TaxAmount, @GrandTotal,
 @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1, @ApprovedBy);";

            var conn = Connection;
            await EnsureOpenAsync(conn);

            using var tx = conn.BeginTransaction();
            try
            {
                // autobind DeliveryDate from quotation if missing
                if ((so.DeliveryDate == null || so.DeliveryDate == default) && so.QuotationNo > 0)
                {
                    var qDel = await GetQuotationDeliveryDateAsync(conn, tx, so.QuotationNo);
                    if (qDel.HasValue) so.DeliveryDate = qDel.Value;
                }

                var soNo = await GetNextSalesOrderNoAsync(conn, tx, "SO-", 4);

                var salesOrderId = await conn.ExecuteScalarAsync<int>(insertHeader, new
                {
                    so.QuotationNo,
                    so.CustomerId,
                    so.RequestedDate,
                    so.DeliveryDate,

                    DeliveryTo = (object?)so.DeliveryTo ?? DBNull.Value,
                    Remarks = (object?)so.Remarks ?? DBNull.Value,

                    so.Status,
                    so.Shipping,
                    so.Discount,
                    so.GstPct,
                    SalesOrderNo = soNo,
                    so.SubTotal,
                    so.TaxAmount,
                    so.GrandTotal,
                    so.CreatedBy,
                    so.CreatedDate,
                    so.UpdatedBy,
                    UpdatedDate = so.UpdatedDate ?? now,
                    ApprovedBy = (object?)so.ApprovedBy ?? DBNull.Value
                }, tx);

                foreach (var l in so.LineItems ?? Enumerable.Empty<SalesOrderLines>())
                {
                    // 1) Insert ORDER line (Quantity = ordered qty)
                    var lineId = await InsertSalesOrderLineAsync(conn, tx, salesOrderId, l, so, now);

                    // 2) Allocation candidates
                    var itemMasterId = await GetItemMasterIdAsync(conn, tx, l.ItemId) ?? 0;
                    if (itemMasterId == 0)
                        throw new InvalidOperationException($"Item master not found for ItemId {l.ItemId}");

                    var cands = await GetAllocCandidatesAsync(conn, tx, itemMasterId);
                    var allocs = MakeAllocation(cands, l.Quantity);

                    // 3) Insert allocations
                    var allocatedQty = allocs.Sum(a => a.Qty);
                    if (allocatedQty > 0)
                        await InsertAllocRowsAsync(conn, tx, lineId, allocs, so, now);

                    // 4) Update line summary (LockedQty = allocatedQty, show first WH/SUP/BIN)
                    await UpdateLineAllocationSummaryAsync(conn, tx, lineId);

                    // 5) Shortage -> PurchaseAlert (NO Quantity=0 row)
                    var shortage = l.Quantity - allocatedQty;
                    if (shortage > 0)
                    {
                        var first = allocs.FirstOrDefault();
                        int? wh = allocs.Count > 0 ? first.WarehouseId : (int?)null;
                        int? sup = allocs.Count > 0 ? first.SupplierId : (int?)null;
                        await UpsertPurchaseAlertAsync(conn, tx, salesOrderId, soNo, l.ItemId, l.ItemName, shortage, wh, sup);
                    }
                }

                await RecomputeAndSetHeaderTotalsAsync(conn, tx, salesOrderId, so.GstPct, so.Shipping, 0m);

                tx.Commit();
                return salesOrderId;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ====================================================
        // ========== UPDATE (FULL REALLOCATION) ===============
        // ====================================================
        public async Task UpdateWithReallocationAsync(SalesOrder so)
        {
            var now = DateTime.UtcNow;

            const string updHead = @"
UPDATE dbo.SalesOrder SET
    RequestedDate = @RequestedDate,
    DeliveryDate  = @DeliveryDate,
    DeliveryTo    = @DeliveryTo,
    Remarks       = @Remarks,
    Shipping      = @Shipping,
    Discount      = @Discount,
    GstPct        = @GstPct,
    UpdatedBy     = @UpdatedBy,
    UpdatedDate   = @UpdatedDate
WHERE Id=@Id AND IsActive=1;";

            const string softDeleteLines = @"
UPDATE dbo.SalesOrderLines
SET IsActive=0, UpdatedBy=@UpdatedBy, UpdatedDate=@UpdatedDate
WHERE SalesOrderId=@SalesOrderId AND IsActive=1;";

            var conn = Connection;
            await EnsureOpenAsync(conn);

            using var tx = conn.BeginTransaction();
            try
            {
                if ((so.DeliveryDate == null || so.DeliveryDate == default) && so.QuotationNo > 0)
                {
                    var qDel = await GetQuotationDeliveryDateAsync(conn, tx, so.QuotationNo);
                    if (qDel.HasValue) so.DeliveryDate = qDel.Value;
                }

                await conn.ExecuteAsync(updHead, new
                {
                    so.RequestedDate,
                    so.DeliveryDate,
                    DeliveryTo = (object?)so.DeliveryTo ?? DBNull.Value,
                    Remarks = (object?)so.Remarks ?? DBNull.Value,
                    so.Shipping,
                    so.Discount,
                    so.GstPct,
                    so.UpdatedBy,
                    UpdatedDate = now,
                    so.Id
                }, tx);

                // deactivate old alloc + lines
                await SoftDeleteAllocBySoAsync(conn, tx, so.Id, so.UpdatedBy ?? so.CreatedBy);
                await conn.ExecuteAsync(softDeleteLines, new
                {
                    SalesOrderId = so.Id,
                    UpdatedBy = so.UpdatedBy,
                    UpdatedDate = now
                }, tx);

                var soNo = await conn.ExecuteScalarAsync<string>(
                    "SELECT TOP (1) SalesOrderNo FROM dbo.SalesOrder WITH (NOLOCK) WHERE Id=@Id;",
                    new { so.Id }, tx) ?? "";

                // re-insert lines + allocs (same as create)
                foreach (var l in so.LineItems ?? Enumerable.Empty<SalesOrderLines>())
                {
                    var lineId = await InsertSalesOrderLineAsync(conn, tx, so.Id, l, so, now);

                    var itemMasterId = await GetItemMasterIdAsync(conn, tx, l.ItemId) ?? 0;
                    if (itemMasterId == 0)
                        throw new InvalidOperationException($"Item master not found for ItemId {l.ItemId}");

                    var cands = await GetAllocCandidatesAsync(conn, tx, itemMasterId);
                    var allocs = MakeAllocation(cands, l.Quantity);

                    var allocatedQty = allocs.Sum(a => a.Qty);
                    if (allocatedQty > 0)
                        await InsertAllocRowsAsync(conn, tx, lineId, allocs, so, now);

                    await UpdateLineAllocationSummaryAsync(conn, tx, lineId);

                    var shortage = l.Quantity - allocatedQty;
                    if (shortage > 0)
                    {
                        var first = allocs.FirstOrDefault();
                        int? wh = allocs.Count > 0 ? first.WarehouseId : (int?)null;
                        int? sup = allocs.Count > 0 ? first.SupplierId : (int?)null;
                        await UpsertPurchaseAlertAsync(conn, tx, so.Id, soNo, l.ItemId, l.ItemName, shortage, wh, sup);
                    }
                }

                await RecomputeAndSetHeaderTotalsAsync(conn, tx, so.Id, so.GstPct, so.Shipping, 0m);

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ====================================================
        // ========== LIGHT UPDATE (no realloc) ================
        // ====================================================
        public async Task UpdateAsync(SalesOrder so)
        {
            var now = DateTime.UtcNow;

            const string updHead = @"
UPDATE dbo.SalesOrder SET
    QuotationNo=@QuotationNo,
    CustomerId=@CustomerId,
    RequestedDate=@RequestedDate,
    DeliveryDate=@DeliveryDate,
    DeliveryTo=@DeliveryTo,
    Remarks=@Remarks,
    Status=@Status,
    Shipping=@Shipping,
    Discount=@Discount,
    GstPct=@GstPct,
    UpdatedBy=@UpdatedBy,
    UpdatedDate=@UpdatedDate
WHERE Id=@Id;";

            const string updLine = @"
UPDATE dbo.SalesOrderLines SET
    ItemId=@ItemId,
    ItemName=@ItemName,
    Uom=@Uom,
    [Description]=@Description,
    Quantity=@Quantity,
    UnitPrice=@UnitPrice,
    Discount=@Discount,
    Tax=@Tax,
    TaxCodeId=@TaxCodeId,
    TaxAmount=@TaxAmount,
    Total=@Total,
    UpdatedBy=@UpdatedBy,
    UpdatedDate=@UpdatedDate,
    IsActive=1
WHERE Id=@Id AND SalesOrderId=@SalesOrderId;";

            var conn = Connection;
            await EnsureOpenAsync(conn);

            using var tx = conn.BeginTransaction();
            try
            {
                if ((so.DeliveryDate == null || so.DeliveryDate == default) && so.QuotationNo > 0)
                {
                    var qDel = await GetQuotationDeliveryDateAsync(conn, tx, so.QuotationNo);
                    if (qDel.HasValue) so.DeliveryDate = qDel.Value;
                }

                await conn.ExecuteAsync(updHead, new
                {
                    so.QuotationNo,
                    so.CustomerId,
                    so.RequestedDate,
                    so.DeliveryDate,

                    DeliveryTo = (object?)so.DeliveryTo ?? DBNull.Value,
                    Remarks = (object?)so.Remarks ?? DBNull.Value,

                    so.Status,
                    so.Shipping,
                    so.Discount,
                    so.GstPct,
                    so.UpdatedBy,
                    UpdatedDate = now,
                    so.Id
                }, tx);

                // We expect UI to send existing line id in l.Id (or DTO -> model mapping)
                foreach (var l in so.LineItems ?? Enumerable.Empty<SalesOrderLines>())
                {
                    if (l.Id <= 0) continue;

                    var (_, tax, computedTotal, _) =
                        ComputeAmounts(l.Quantity, l.UnitPrice, l.Discount, l.Tax ?? "EXEMPT", so.GstPct);

                    await conn.ExecuteAsync(updLine, new
                    {
                        Id = l.Id,
                        SalesOrderId = so.Id,
                        l.ItemId,
                        l.ItemName,
                        l.Uom,
                        Description = (object?)l.Description ?? DBNull.Value,
                        Quantity = l.Quantity,
                        UnitPrice = l.UnitPrice,
                        Discount = l.Discount,
                        Tax = l.Tax,
                        l.TaxCodeId,
                        TaxAmount = tax,
                        Total = computedTotal,
                        UpdatedBy = so.UpdatedBy,
                        UpdatedDate = now
                    }, tx);
                }

                await RecomputeAndSetHeaderTotalsAsync(conn, tx, so.Id, so.GstPct, so.Shipping, 0m);

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ====================================================
        // ===================== SOFT DELETE ==================
        // ====================================================
        public async Task DeactivateAsync(int id, int updatedBy)
        {
            const string sqlHead = @"UPDATE dbo.SalesOrder SET IsActive=0, UpdatedBy=@UpdatedBy, UpdatedDate=SYSUTCDATETIME() WHERE Id=@Id;";
            const string sqlLines = @"UPDATE dbo.SalesOrderLines SET IsActive=0, UpdatedBy=@UpdatedBy, UpdatedDate=SYSUTCDATETIME() WHERE SalesOrderId=@Id AND IsActive=1;";
            const string sqlAlloc = @"
UPDATE a SET IsActive=0, UpdatedBy=@UpdatedBy, UpdatedDate=SYSUTCDATETIME()
FROM dbo.SalesOrderLineAlloc a
JOIN dbo.SalesOrderLines l ON l.Id=a.SalesOrderLineId
WHERE l.SalesOrderId=@Id AND a.IsActive=1;";

            var conn = Connection;
            await EnsureOpenAsync(conn);

            using var tx = conn.BeginTransaction();
            try
            {
                var n = await conn.ExecuteAsync(sqlHead, new { Id = id, UpdatedBy = updatedBy }, tx);
                if (n == 0) throw new KeyNotFoundException("Sales Order not found.");

                await conn.ExecuteAsync(sqlLines, new { Id = id, UpdatedBy = updatedBy }, tx);
                await conn.ExecuteAsync(sqlAlloc, new { Id = id, UpdatedBy = updatedBy }, tx);

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ====================================================
        // =============== QUOTATION → DETAILS =================
        // ====================================================
        public async Task<QutationDetailsViewInfo?> GetByQuatitonDetails(int id)
        {
            const string sql = @"
SELECT q.Id,
       q.Number,
       q.Status,
       q.CustomerId,
       c.CustomerName AS CustomerName,
       q.CurrencyId,
       q.FxRate,
       q.PaymentTermsId,
       q.DeliveryDate,
       q.Subtotal,
       q.TaxAmount,
       q.Rounding,
       q.GrandTotal,
       q.NeedsHodApproval,
       cu.CurrencyName,
       pt.PaymentTermsName,
       q.DeliveryTo,
	   q.Remarks,
       COALESCE(cn.GSTPercentage,0) AS GstPct
FROM dbo.Quotation q
LEFT JOIN dbo.Customer     c  ON c.Id = q.CustomerId
LEFT JOIN dbo.Currency     cu ON cu.Id = q.CurrencyId
LEFT JOIN dbo.PaymentTerms pt ON pt.Id = q.PaymentTermsId
LEFT JOIN dbo.Location     ln ON ln.Id = c.LocationId
LEFT JOIN dbo.Country      cn ON cn.Id = c.CountryId
WHERE q.Id = @Id AND q.IsActive = 1;

SELECT l.Id,
       l.QuotationId,
       l.ItemId,
       i.ItemName AS ItemName,
       l.UomId,
       u.Name AS UomName,
       l.Qty,
       l.UnitPrice,
       l.DiscountPct,
       l.TaxMode,
       l.LineNet,
       l.LineTax,
       l.LineTotal,
       l.Description,
       whAgg.WarehouseCount,
       whAgg.WarehouseIdsCsv AS WarehouseIds,
       whAgg.WarehousesJson
FROM dbo.QuotationLine l
LEFT JOIN dbo.Item i  ON i.Id = l.ItemId
LEFT JOIN dbo.Uom  u  ON u.Id = l.UomId
OUTER APPLY (
    SELECT im.Id AS ItemMasterId
    FROM dbo.ItemMaster im
    WHERE im.Sku = i.ItemCode
) AS IMX
OUTER APPLY (
    SELECT
      COUNT(DISTINCT W.WarehouseId) AS WarehouseCount,
      STRING_AGG(CAST(W.WarehouseId AS varchar(20)), ',') AS WarehouseIdsCsv,
      (
        SELECT 
            W.WarehouseId,
            wh.Name as WarehouseName,
            SUM(W.OnHand)   AS OnHand,
            SUM(W.Reserved) AS Reserved,
            CASE 
              WHEN SUM(W.OnHand - W.Reserved) - ISNULL(Lck.LckQty,0) < 0 
                   THEN 0 
              ELSE SUM(W.OnHand - W.Reserved) - ISNULL(Lck.LckQty,0) 
            END AS Available
        FROM dbo.ItemWarehouseStock W WITH (NOLOCK)
        JOIN dbo.Warehouse wh WITH (NOLOCK) ON wh.Id = W.WarehouseId
        OUTER APPLY (
            SELECT SUM(sol.LockedQty) AS LckQty
            FROM dbo.SalesOrderLines sol WITH (NOLOCK)
            JOIN dbo.SalesOrder so  WITH (NOLOCK) ON so.Id = sol.SalesOrderId AND so.IsActive = 1
            JOIN dbo.Item i2        WITH (NOLOCK) ON i2.Id = sol.ItemId
            JOIN dbo.ItemMaster im2 WITH (NOLOCK) ON im2.Sku = i2.ItemCode
            WHERE sol.IsActive = 1
              AND im2.Id = IMX.ItemMasterId
              AND sol.WarehouseId = W.WarehouseId
        ) Lck
        WHERE W.ItemId = IMX.ItemMasterId
        GROUP BY W.WarehouseId, wh.Name, ISNULL(Lck.LckQty,0)
        FOR JSON PATH
      ) AS WarehousesJson
    FROM dbo.ItemWarehouseStock W WITH (NOLOCK)
    WHERE W.ItemId = IMX.ItemMasterId
) AS whAgg
WHERE l.QuotationId = @Id
ORDER BY l.Id;";

            using var multi = await Connection.QueryMultipleAsync(sql, new { Id = id });

            var head = await multi.ReadFirstOrDefaultAsync<QutationDetailsViewInfo>();
            if (head is null) return null;

            var lines = (await multi.ReadAsync<QutationDetailsViewInfo.QuotationLineDetailsViewInfo>()).ToList();

            foreach (var l in lines)
            {
                if (!string.IsNullOrWhiteSpace(l.WarehousesJson))
                {
                    try
                    {
                        l.Warehouses = JsonSerializer.Deserialize<List<QutationDetailsViewInfo.WarehouseInfoDTO>>(l.WarehousesJson) ?? new();
                    }
                    catch { l.Warehouses = new(); }
                }
                else l.Warehouses = new();
            }

            head.Lines = lines;
            return head;
        }

        // ====================================================
        // ================= PREVIEW (READ ONLY) ===============
        // ====================================================
        public async Task<AllocationPreviewResponse> PreviewAllocationAsync(AllocationPreviewRequest req)
        {
            var result = new AllocationPreviewResponse();
            if (req?.Lines == null || req.Lines.Count == 0)
                return result;

            var conn = Connection;
            await EnsureOpenAsync(conn);

            const string sqlMapItemMaster = @"
SELECT TOP 1 im.Id
FROM dbo.Item i WITH (NOLOCK)
JOIN dbo.ItemMaster im WITH (NOLOCK) ON im.Sku = i.ItemCode
WHERE i.Id = @ItemId;";

            const string sqlPreviewCandidates = @"
WITH IWS AS (
    SELECT W.WarehouseId,
           W.BinId,
           W.ItemId,
           SUM(W.OnHand)   AS OnHand,
           SUM(W.Reserved) AS Reserved
    FROM dbo.ItemWarehouseStock W WITH (NOLOCK)
    WHERE W.ItemId = @ItemMasterId
    GROUP BY W.WarehouseId, W.BinId, W.ItemId
),
LCK AS (
    SELECT sol.WarehouseId, SUM(sol.LockedQty) AS LckQty
    FROM dbo.SalesOrderLines sol WITH (NOLOCK)
    JOIN dbo.SalesOrder so  WITH (NOLOCK) ON so.Id = sol.SalesOrderId AND so.IsActive = 1
    JOIN dbo.Item i        WITH (NOLOCK) ON i.Id  = sol.ItemId
    JOIN dbo.ItemMaster im WITH (NOLOCK) ON im.Sku = i.ItemCode
    WHERE sol.IsActive = 1
      AND im.Id = @ItemMasterId
    GROUP BY sol.WarehouseId
)
SELECT 
    ip.WarehouseId,
    w.Name      AS WarehouseName,
    iws.BinId,
    b.BinName,
    ip.SupplierId,
    s.Name As SupplierName,
    CASE 
      WHEN iws.ItemId IS NULL THEN 0
      ELSE CASE 
             WHEN (ISNULL(iws.OnHand,0) - ISNULL(iws.Reserved,0)) - ISNULL(lck.LckQty,0) < 0 
                  THEN 0 
             ELSE (ISNULL(iws.OnHand,0) - ISNULL(iws.Reserved,0)) - ISNULL(lck.LckQty,0)
           END
    END AS WhAvail,
    ip.Qty      AS SupplierQty
FROM dbo.ItemPrice ip WITH (NOLOCK)
LEFT JOIN IWS iws ON iws.ItemId = ip.ItemId AND iws.WarehouseId = ip.WarehouseId
LEFT JOIN LCK lck ON lck.WarehouseId = ip.WarehouseId
LEFT JOIN dbo.Warehouse w WITH (NOLOCK) ON w.Id = ip.WarehouseId
LEFT JOIN dbo.Suppliers s WITH (NOLOCK) ON s.Id = ip.SupplierId
LEFT JOIN dbo.Bin b WITH (NOLOCK)       ON b.Id = iws.BinId
WHERE ip.ItemId = @ItemMasterId
  AND ip.Qty > 0
ORDER BY WhAvail DESC, ip.Qty DESC;";

            foreach (var line in req.Lines)
            {
                var lineRes = new AllocationPreviewLineResult
                {
                    ItemId = line.ItemId,
                    RequestedQty = line.Quantity,
                    AllocatedQty = 0,
                    FullyAllocated = false,
                    Allocations = new List<AllocPiece>()
                };

                var itemMasterId = await conn.ExecuteScalarAsync<int?>(sqlMapItemMaster, new { line.ItemId });
                if (itemMasterId is null || itemMasterId.Value <= 0)
                {
                    result.Lines.Add(lineRes);
                    continue;
                }

                var raw = (await conn.QueryAsync(sqlPreviewCandidates, new { ItemMasterId = itemMasterId.Value }))
                    .Select(r => new
                    {
                        WarehouseId = (int)r.WarehouseId,
                        WarehouseName = (string?)r.WarehouseName,
                        BinId = (int?)r.BinId,
                        BinName = (string?)r.BinName,
                        SupplierId = (int)r.SupplierId,
                        SupplierName = (string?)r.SupplierName,
                        WhAvail = (decimal)r.WhAvail,
                        SupplierQty = (decimal)r.SupplierQty
                    })
                    .ToList();

                var nameMap = raw.ToDictionary(
                    k => (k.WarehouseId, k.SupplierId, k.BinId),
                    v => (v.WarehouseName, v.SupplierName, v.BinName));

                var cands = raw.Select(r =>
                    new AllocCandidate(r.WarehouseId, r.BinId, r.SupplierId, r.WhAvail, r.SupplierQty)).ToList();

                var allocs = MakeAllocation(cands, line.Quantity);
                var allocatedQty = allocs.Sum(a => a.Qty);

                lineRes.AllocatedQty = allocatedQty;
                lineRes.FullyAllocated = allocatedQty >= line.Quantity;

                foreach (var a in allocs)
                {
                    nameMap.TryGetValue((a.WarehouseId, a.SupplierId, a.BinId), out var names);
                    lineRes.Allocations.Add(new AllocPiece
                    {
                        WarehouseId = a.WarehouseId,
                        SupplierId = a.SupplierId,
                        BinId = a.BinId,
                        Qty = a.Qty,
                        WarehouseName = names.WarehouseName,
                        SupplierName = names.SupplierName,
                        BinName = names.BinName
                    });
                }

                result.Lines.Add(lineRes);
            }

            return result;
        }

        // ====================================================
        // ===================== APPROVE / REJECT ==============
        // ====================================================
        public async Task<int> ApproveAsync(int id, int approvedBy)
        {
            const string sql = @"
UPDATE dbo.SalesOrder
SET Status = 2,
    ApprovedBy = @ApprovedBy,
    UpdatedBy = @ApprovedBy,
    UpdatedDate = SYSUTCDATETIME()
WHERE Id = @Id AND IsActive = 1;";

            var conn = Connection;
            await EnsureOpenAsync(conn);

            var rows = await conn.ExecuteAsync(sql, new { Id = id, ApprovedBy = approvedBy });
            return rows;
        }

        public async Task<int> RejectAsync(int id)
        {
            const string sqlHead = @"
UPDATE dbo.SalesOrder
SET IsActive = 0,
    Status = 4,
    UpdatedDate = SYSUTCDATETIME()
WHERE Id = @Id;";

            const string sqlLines = @"
UPDATE dbo.SalesOrderLines
SET IsActive = 0,
    LockedQty = 0,
    UpdatedDate = SYSUTCDATETIME()
WHERE SalesOrderId = @Id AND IsActive = 1;";

            const string sqlAlloc = @"
UPDATE a
SET IsActive=0,
    UpdatedDate=SYSUTCDATETIME()
FROM dbo.SalesOrderLineAlloc a
JOIN dbo.SalesOrderLines l ON l.Id=a.SalesOrderLineId
WHERE l.SalesOrderId=@Id AND a.IsActive=1;";

            var conn = Connection;
            await EnsureOpenAsync(conn);

            using var tx = conn.BeginTransaction();
            try
            {
                var a = await conn.ExecuteAsync(sqlHead, new { Id = id }, tx);
                if (a == 0)
                {
                    tx.Rollback();
                    return 0;
                }

                var b = await conn.ExecuteAsync(sqlLines, new { Id = id }, tx);
                await conn.ExecuteAsync(sqlAlloc, new { Id = id }, tx);

                tx.Commit();
                return a + b;
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ====================================================
        // ===================== DRAFT LINES ===================
        // ====================================================
        public async Task<IEnumerable<DraftLineDTO>> GetDraftLinesAsync()
        {
            const string sql = @"
SELECT
    so.Id                AS SalesOrderId,
    so.SalesOrderNo,
    sol.Id               AS LineId,
    sol.ItemId,
    ISNULL(sol.ItemName,'') AS ItemName,
    sol.Uom,
    ISNULL(sol.Quantity,0)  AS Quantity,
    sol.UnitPrice,
    sol.WarehouseId,
    sol.BinId,
    sol.SupplierId,
    sol.LockedQty,
    sol.CreatedDate
FROM dbo.SalesOrderLines sol WITH (NOLOCK)
JOIN dbo.SalesOrder so       WITH (NOLOCK) ON so.Id = sol.SalesOrderId
WHERE sol.IsActive = 1
  AND so.IsActive  = 1
  AND (sol.WarehouseId IS NULL OR sol.SupplierId IS NULL OR sol.BinId IS NULL)
ORDER BY so.Id DESC, sol.Id DESC;";

            return await Connection.QueryAsync<DraftLineDTO>(sql);
        }

        // ====================================================
        // ===================== GET BY STATUS =================
        // ====================================================
        public async Task<IEnumerable<SalesOrderDTO>> GetAllByStatusAsync(byte status)
        {
            const string headersSql = @"
SELECT
    so.Id,
    so.QuotationNo,
    so.CustomerId,
    ISNULL(c.CustomerName,'') AS CustomerName,
    so.RequestedDate,
    so.DeliveryDate,

    so.DeliveryTo,
    so.Remarks,

    so.Status,
    so.Shipping,
    so.Discount,
    so.GstPct,
    so.CreatedBy,
    so.CreatedDate,
    so.UpdatedBy,
    so.UpdatedDate,
    so.IsActive,
    so.SalesOrderNo,
    ISNULL(so.Subtotal,0)    AS Subtotal,
    ISNULL(so.TaxAmount,0)   AS TaxAmount,
    ISNULL(so.GrandTotal,0)  AS GrandTotal,
    so.ApprovedBy
FROM dbo.SalesOrder so
LEFT JOIN dbo.Customer c ON c.Id = so.CustomerId
WHERE so.IsActive = 1
  AND so.Status   = @Status
ORDER BY so.Id;";

            var headers = (await Connection.QueryAsync<SalesOrderDTO>(headersSql, new { Status = status })).ToList();
            if (headers.Count == 0) return headers;

            var ids = headers.Select(h => h.Id).ToArray();

            const string linesSql = @"
SELECT
    Id,
    SalesOrderId,
    ItemId,
    ItemName,
    Uom,
    [Description],
    Quantity,
    UnitPrice,
    Discount,
    Tax,
    TaxCodeId,
    TaxAmount,
    Total,
    WarehouseId,
    BinId,
    Available,
    SupplierId,
    LockedQty,
    CreatedBy,
    CreatedDate,
    UpdatedBy,
    UpdatedDate,
    IsActive
FROM dbo.SalesOrderLines
WHERE SalesOrderId IN @Ids
  AND IsActive = 1;";

            var lines = await Connection.QueryAsync<SalesOrderLineDTO>(linesSql, new { Ids = ids });

            var map = headers.ToDictionary(h => h.Id);
            foreach (var ln in lines)
                if (map.TryGetValue(ln.SalesOrderId, out var parent))
                    parent.LineItems.Add(ln);

            return headers;
        }

        // ====================================================
        // ===================== OPEN BY CUSTOMER ==============
        // ====================================================
        public async Task<IEnumerable<SalesOrderListDto>> GetOpenByCustomerAsync(int customerId)
        {
            const string sql = "sp_SalesOrder_GetOpenByCustomer";
            return await Connection.QueryAsync<SalesOrderListDto>(
                sql,
                new { CustomerId = customerId },
                commandType: CommandType.StoredProcedure
            );
        }

        // ====================================================
        // ===================== ALERT UPSERT ==================
        // ====================================================
        private async Task UpsertPurchaseAlertAsync(
            IDbConnection conn, IDbTransaction tx,
            int salesOrderId, string soNo,
            int itemId, string itemName, decimal shortageQty,
            int? warehouseId, int? supplierId)
        {
            if (shortageQty <= 0) return;

            string? whName = warehouseId.HasValue
                ? await conn.ExecuteScalarAsync<string?>("SELECT Name FROM dbo.Warehouse WITH (NOLOCK) WHERE Id=@Id;", new { Id = warehouseId.Value }, tx)
                : null;

            string? supName = supplierId.HasValue
                ? await conn.ExecuteScalarAsync<string?>("SELECT Name FROM dbo.Suppliers WITH (NOLOCK) WHERE Id=@Id;", new { Id = supplierId.Value }, tx)
                : null;

            var itemLabel = string.IsNullOrWhiteSpace(itemName) ? $"#{itemId}" : itemName;
            var title = $"Sales shortage — Item: {itemLabel} · Qty: {shortageQty}"
                      + (warehouseId != null ? $" · WH: {(whName ?? warehouseId.ToString())}" : "")
                      + (supplierId != null ? $" · SUP: {(supName ?? supplierId.ToString())}" : "");

            const string selSql = @"
SELECT TOP 1 Id
FROM dbo.PurchaseAlert WITH (UPDLOCK, HOLDLOCK)
WHERE Source='SO' AND SourceId=@SourceId AND ItemId=@ItemId AND IsRead=0
ORDER BY Id DESC;";

            var existingId = await conn.ExecuteScalarAsync<int?>(
                selSql, new { SourceId = salesOrderId, ItemId = itemId }, tx);

            if (existingId.HasValue)
            {
                const string updSql = @"UPDATE dbo.PurchaseAlert
SET RequiredQty=@RequiredQty, Message=@Message
WHERE Id=@Id;";
                await conn.ExecuteAsync(updSql, new
                {
                    Id = existingId.Value,
                    RequiredQty = shortageQty,
                    Message = title
                }, tx);
            }
            else
            {
                const string insSql = @"
INSERT INTO dbo.PurchaseAlert
(ItemId, ItemName, RequiredQty, WarehouseId, SupplierId,
 Source, SourceId, SourceNo, Message, IsRead, CreatedDate)
VALUES
(@ItemId, @ItemName, @RequiredQty, @WarehouseId, @SupplierId,
 'SO', @SourceId, @SourceNo, @Message, 0, SYSUTCDATETIME());";

                await conn.ExecuteAsync(insSql, new
                {
                    ItemId = itemId,
                    ItemName = (object?)itemName ?? DBNull.Value,
                    RequiredQty = shortageQty,
                    WarehouseId = warehouseId,
                    SupplierId = supplierId,
                    SourceId = salesOrderId,
                    SourceNo = soNo,
                    Message = title
                }, tx);
            }
        }
    }
}
