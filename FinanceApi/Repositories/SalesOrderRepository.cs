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

    so.LineSourceId,   -- ✅ NEW

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
            // ✅ Header + ItemSet aggregate summary
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

    so.LineSourceId, -- ✅ NEW

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

    -- ✅ NEW: ItemSet summary for SO header
    ISNULL(isAgg.ItemSetCount, 0)    AS ItemSetCount,
    ISNULL(isAgg.ItemSetIdsCsv, '')  AS ItemSetIds,
    ISNULL(isAgg.ItemSetsJson, '[]') AS ItemSetsJson

FROM dbo.SalesOrder so
LEFT JOIN dbo.Customer  c ON c.Id = so.CustomerId
LEFT JOIN dbo.Quotation q ON q.Id = so.QuotationNo

OUTER APPLY (
    SELECT
        COUNT(*) AS ItemSetCount,
        STRING_AGG(CAST(som.ItemSetId AS varchar(20)), ',') AS ItemSetIdsCsv,
        (
            SELECT
                som.Id,
                som.SalesOrderId,
                som.ItemSetId,
                iset.SetName AS ItemSetName,
                som.CreatedBy,
                som.CreatedDate
            FROM dbo.SalesOrderItemSetMap som
            LEFT JOIN dbo.ItemSet iset ON iset.Id = som.ItemSetId
            WHERE som.SalesOrderId = so.Id
              AND ISNULL(som.IsActive,1)=1
            FOR JSON PATH
        ) AS ItemSetsJson
    FROM dbo.SalesOrderItemSetMap som
    WHERE som.SalesOrderId = so.Id
      AND ISNULL(som.IsActive,1)=1
) isAgg

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
        // ===================== HELPERS ======================
        // ====================================================
        private async Task EnsureOpenAsync(IDbConnection conn)
        {
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();
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

        // ======= insert SO line (NO allocation fields) =======
        private async Task<int> InsertSalesOrderLineAsync(
            IDbConnection conn, IDbTransaction tx,
            int salesOrderId, SalesOrderLines l, SalesOrder so, DateTime now)
        {
            var (_, tax, total, _) = ComputeAmounts(
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

        // ✅ NEW: ItemSet mapping table upsert (soft delete + insert)
        private async Task UpsertSalesOrderItemSetsAsync(
            IDbConnection conn, IDbTransaction tx,
            int salesOrderId, IEnumerable<int>? itemSetIds,
            int? userId, DateTime now)
        {
            // 1) Soft delete existing
            const string soft = @"
UPDATE dbo.SalesOrderItemSetMap
SET IsActive=0,
    UpdatedBy=@UpdatedBy,
    UpdatedDate=@UpdatedDate
WHERE SalesOrderId=@SalesOrderId
  AND ISNULL(IsActive,1)=1;";
            await conn.ExecuteAsync(soft, new
            {
                SalesOrderId = salesOrderId,
                UpdatedBy = userId,
                UpdatedDate = now
            }, tx);

            var ids = (itemSetIds ?? Enumerable.Empty<int>())
                .Select(x => Convert.ToInt32(x))
                .Where(x => x > 0)
                .Distinct()
                .ToList();

            if (ids.Count == 0) return;

            // 2) Insert new active rows
            const string ins = @"
INSERT INTO dbo.SalesOrderItemSetMap
(SalesOrderId, ItemSetId, IsActive, CreatedBy, CreatedDate, UpdatedBy, UpdatedDate)
VALUES
(@SalesOrderId, @ItemSetId, 1, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate);";

            foreach (var itemSetId in ids)
            {
                await conn.ExecuteAsync(ins, new
                {
                    SalesOrderId = salesOrderId,
                    ItemSetId = itemSetId,
                    CreatedBy = userId,
                    CreatedDate = now,
                    UpdatedBy = userId,
                    UpdatedDate = now
                }, tx);
            }
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
 LineSourceId,                 -- ✅ NEW
 Status, Shipping, Discount, GstPct,
 SalesOrderNo, SubTotal, TaxAmount, GrandTotal,
 CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive, ApprovedBy)
OUTPUT INSERTED.Id
VALUES
(@QuotationNo, @CustomerId, @RequestedDate, @DeliveryDate,
 @DeliveryTo, @Remarks,
 @LineSourceId,
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

                    LineSourceId = so.LineSourceId,  // ✅ NEW

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

                // ✅ NEW: Save ItemSet mapping (only if LineSourceId = 2)
                if (Convert.ToInt32(so.LineSourceId) == 2)
                {
                    // Expect service to map payload `itemSetIds` -> so.ItemSetIds (List<int>)
                    await UpsertSalesOrderItemSetsAsync(conn, tx, salesOrderId, so.ItemSetIds, so.CreatedBy, now);
                }
                else
                {
                    // ensure cleared
                    await UpsertSalesOrderItemSetsAsync(conn, tx, salesOrderId, Enumerable.Empty<int>(), so.CreatedBy, now);
                }

                // Insert SO lines (NO allocation)
                foreach (var l in so.LineItems ?? Enumerable.Empty<SalesOrderLines>())
                {
                    await InsertSalesOrderLineAsync(conn, tx, salesOrderId, l, so, now);
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
        // ========== UPDATE (REBUILD LINES) ===================
        // ====================================================
        public async Task UpdateWithReallocationAsync(SalesOrder so)
        {
            // ✅ allocation removed, but we keep method name to avoid breaking interface
            var now = DateTime.UtcNow;

            const string updHead = @"
UPDATE dbo.SalesOrder SET
    RequestedDate = @RequestedDate,
    DeliveryDate  = @DeliveryDate,
    DeliveryTo    = @DeliveryTo,
    Remarks       = @Remarks,
    LineSourceId  = @LineSourceId,  -- ✅ NEW
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

                    LineSourceId = so.LineSourceId, // ✅ NEW

                    so.Shipping,
                    so.Discount,
                    so.GstPct,
                    so.UpdatedBy,
                    UpdatedDate = now,
                    so.Id
                }, tx);

                // ✅ ItemSet mapping update
                if (Convert.ToInt32(so.LineSourceId) == 2)
                    await UpsertSalesOrderItemSetsAsync(conn, tx, so.Id, so.ItemSetIds, so.UpdatedBy ?? so.CreatedBy, now);
                else
                    await UpsertSalesOrderItemSetsAsync(conn, tx, so.Id, Enumerable.Empty<int>(), so.UpdatedBy ?? so.CreatedBy, now);

                // deactivate old lines
                await conn.ExecuteAsync(softDeleteLines, new
                {
                    SalesOrderId = so.Id,
                    UpdatedBy = so.UpdatedBy,
                    UpdatedDate = now
                }, tx);

                // re-insert lines
                foreach (var l in so.LineItems ?? Enumerable.Empty<SalesOrderLines>())
                {
                    await InsertSalesOrderLineAsync(conn, tx, so.Id, l, so, now);
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
        // ========== LIGHT UPDATE (existing lines only) =======
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
    LineSourceId=@LineSourceId,   -- ✅ NEW
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

                    LineSourceId = so.LineSourceId, // ✅ NEW

                    so.Status,
                    so.Shipping,
                    so.Discount,
                    so.GstPct,
                    so.UpdatedBy,
                    UpdatedDate = now,
                    so.Id
                }, tx);

                // ✅ ItemSet mapping update
                if (Convert.ToInt32(so.LineSourceId) == 2)
                    await UpsertSalesOrderItemSetsAsync(conn, tx, so.Id, so.ItemSetIds, so.UpdatedBy ?? so.CreatedBy, now);
                else
                    await UpsertSalesOrderItemSetsAsync(conn, tx, so.Id, Enumerable.Empty<int>(), so.UpdatedBy ?? so.CreatedBy, now);

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
            const string sqlItemSets = @"
UPDATE dbo.SalesOrderItemSetMap
SET IsActive=0, UpdatedBy=@UpdatedBy, UpdatedDate=SYSUTCDATETIME()
WHERE SalesOrderId=@Id AND ISNULL(IsActive,1)=1;";

            var conn = Connection;
            await EnsureOpenAsync(conn);

            using var tx = conn.BeginTransaction();
            try
            {
                var n = await conn.ExecuteAsync(sqlHead, new { Id = id, UpdatedBy = updatedBy }, tx);
                if (n == 0) throw new KeyNotFoundException("Sales Order not found.");

                await conn.ExecuteAsync(sqlLines, new { Id = id, UpdatedBy = updatedBy }, tx);
                await conn.ExecuteAsync(sqlItemSets, new { Id = id, UpdatedBy = updatedBy }, tx);

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
            // ✅ Keep as-is (Quotation side). Allocation concept not here.
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
       q.LineSourceId,
       COALESCE(cn.GSTPercentage,0) AS GstPct,

       ISNULL(isAgg.ItemSetCount, 0)    AS ItemSetCount,
       ISNULL(isAgg.ItemSetIdsCsv, '')  AS ItemSetIds,
       ISNULL(isAgg.ItemSetsJson, '[]') AS ItemSetsJson

FROM dbo.Quotation q
LEFT JOIN dbo.Customer     c  ON c.Id = q.CustomerId
LEFT JOIN dbo.Currency     cu ON cu.Id = q.CurrencyId
LEFT JOIN dbo.PaymentTerms pt ON pt.Id = q.PaymentTermsId
LEFT JOIN dbo.Location     ln ON ln.Id = c.LocationId
LEFT JOIN dbo.Country      cn ON cn.Id = c.CountryId

OUTER APPLY (
    SELECT
        COUNT(*) AS ItemSetCount,
        STRING_AGG(CAST(qis.ItemSetId AS varchar(20)), ',') AS ItemSetIdsCsv,
        (
            SELECT
                qis.Id,
                qis.QuotationId,
                qis.ItemSetId,
                iset.SetName AS ItemSetName,
                qis.CreatedBy,
                qis.CreatedDate
            FROM Finance.dbo.QuotationItemSet qis
            LEFT JOIN Finance.dbo.ItemSet iset ON iset.Id = qis.ItemSetId
            WHERE qis.QuotationId = q.Id
              AND ISNULL(qis.IsActive,1)=1
            FOR JSON PATH
        ) AS ItemSetsJson
    FROM Finance.dbo.QuotationItemSet qis
    WHERE qis.QuotationId = q.Id
      AND ISNULL(qis.IsActive,1)=1
) isAgg

WHERE q.Id = @Id
  AND q.IsActive = 1;

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
              WHEN SUM(W.OnHand - W.Reserved) < 0 THEN 0
              ELSE SUM(W.OnHand - W.Reserved)
            END AS Available
        FROM dbo.ItemWarehouseStock W WITH (NOLOCK)
        JOIN dbo.Warehouse wh WITH (NOLOCK) ON wh.Id = W.WarehouseId
        WHERE W.ItemId = IMX.ItemMasterId
        GROUP BY W.WarehouseId, wh.Name
        FOR JSON PATH
      ) AS WarehousesJson
    FROM dbo.ItemWarehouseStock W WITH (NOLOCK)
    WHERE W.ItemId = IMX.ItemMasterId
) AS whAgg
WHERE l.QuotationId = @Id
ORDER BY l.Id;

SELECT
    qis.Id,
    qis.QuotationId,
    qis.ItemSetId,
    iset.SetName AS ItemSetName,
    qis.CreatedBy,
    qis.CreatedDate,
    qis.IsActive
FROM Finance.dbo.QuotationItemSet qis
LEFT JOIN Finance.dbo.ItemSet iset ON iset.Id = qis.ItemSetId
WHERE qis.QuotationId = @Id
  AND ISNULL(qis.IsActive,1)=1
ORDER BY qis.Id;";

            using var multi = await Connection.QueryMultipleAsync(sql, new { Id = id });

            var head = await multi.ReadFirstOrDefaultAsync<QutationDetailsViewInfo>();
            if (head is null) return null;

            var lines = (await multi.ReadAsync<QutationDetailsViewInfo.QuotationLineDetailsViewInfo>()).ToList();
            head.Lines = lines;
            return head;
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

            const string sqlItemSets = @"
UPDATE dbo.SalesOrderItemSetMap
SET IsActive=0, UpdatedDate=SYSUTCDATETIME()
WHERE SalesOrderId=@Id AND ISNULL(IsActive,1)=1;";

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
                await conn.ExecuteAsync(sqlItemSets, new { Id = id }, tx);

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
            // ✅ Allocation removed, so Draft means Status=0 (Draft) only
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
  AND so.Status    = 0
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

    so.LineSourceId, -- ✅ NEW

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
        // ===================== PREVIEW (REMOVED) ==============
        // ====================================================
        public Task<AllocationPreviewResponse> PreviewAllocationAsync(AllocationPreviewRequest req)
        {
            // ✅ Allocation concept removed
            return Task.FromResult(new AllocationPreviewResponse());
        }
    }
}
