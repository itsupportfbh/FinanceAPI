using System.Data;
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.Data.SqlClient;

namespace FinanceApi.Repositories
{
    public class CreditNoteRepository : DynamicRepository, ICreditNoteRepository
    {
        public CreditNoteRepository(IDbConnectionFactory connectionFactory) : base(connectionFactory) { }

        // ===================== LIST =====================
        public async Task<IEnumerable<CreditNoteDTO>> GetAllAsync()
        {
            const string hdrSql = @"
SELECT
    cn.Id, cn.CreditNoteNo, cn.DoId, d.DoNumber,
    cn.SiId, si.InvoiceNo AS SiNumber,
    cn.CustomerId, ISNULL(c.CustomerName,'') AS CustomerName,
    cn.CreditNoteDate,
    CAST(cn.Status AS tinyint) AS Status,
    ISNULL(cn.Subtotal,0) AS Subtotal,
    cn.IsActive
FROM dbo.CreditNote cn
LEFT JOIN dbo.DeliveryOrder d    ON d.Id = cn.DoId
LEFT JOIN dbo.SalesInvoice si    ON si.Id = cn.SiId
LEFT JOIN dbo.Customer c         ON c.Id = cn.CustomerId
WHERE cn.IsActive = 1
ORDER BY cn.Id DESC;";

            var headers = (await Connection.QueryAsync<CreditNoteDTO>(hdrSql)).ToList();
            if (headers.Count == 0) return headers;

            var ids = headers.Select(h => h.Id).ToArray();

            const string lineSql = @"
SELECT
    l.Id, l.CreditNoteId,
    l.DoId AS DId, l.SiId,l.DoLineId,
    l.ItemId, l.ItemName, l.Uom,
    l.DeliveredQty, l.ReturnedQty,
    l.UnitPrice, l.DiscountPct,l.GstPct,l.Tax,l.TaxCodeId,
    l.LineNet, l.ReasonId, l.RestockDispositionId,
    l.WarehouseId, l.SupplierId, l.BinId,
    l.IsActive
FROM dbo.CreditNoteLine l
WHERE l.CreditNoteId IN @Ids AND l.IsActive = 1
ORDER BY l.Id;";

            var lines = (await Connection.QueryAsync<CreditNoteLineDTO>(lineSql, new { Ids = ids })).ToList();
            var map = headers.ToDictionary(h => h.Id);
            foreach (var ln in lines)
                if (map.TryGetValue(ln.CreditNoteId, out var parent)) parent.Lines.Add(ln);

            return headers;
        }

        // ===================== GET ONE =====================
        public async Task<CreditNoteDTO?> GetByIdAsync(int id)
        {
            const string hdrSql = @"
SELECT TOP (1)
    cn.Id, cn.CreditNoteNo, cn.DoId, d.DoNumber,
    cn.SiId, si.InvoiceNo AS SiNumber,
    cn.CustomerId, ISNULL(c.CustomerName,'') AS CustomerName,
    cn.CreditNoteDate,
    CAST(cn.Status AS tinyint) AS Status,
    ISNULL(cn.Subtotal,0) AS Subtotal,
    cn.IsActive
FROM dbo.CreditNote cn
LEFT JOIN dbo.DeliveryOrder d ON d.Id = cn.DoId
LEFT JOIN dbo.SalesInvoice si ON si.Id = cn.SiId
LEFT JOIN dbo.Customer c      ON c.Id = cn.CustomerId
WHERE cn.Id = @Id AND cn.IsActive = 1;";

            var head = await Connection.QueryFirstOrDefaultAsync<CreditNoteDTO>(hdrSql, new { Id = id });
            if (head is null) return null;

            const string lineSql = @"
SELECT
    l.Id, l.CreditNoteId,
    l.DoId AS DId, l.SiId, l.DoLineId,
    l.ItemId, l.ItemName, l.Uom,
    l.DeliveredQty, l.ReturnedQty,
    l.UnitPrice, l.DiscountPct,l.GstPct,l.Tax,l.TaxCodeId,
    l.LineNet, l.ReasonId, l.RestockDispositionId,
    l.WarehouseId,l.SupplierId,l.BinId,
    l.IsActive
FROM dbo.CreditNoteLine l
WHERE l.CreditNoteId = @Id AND l.IsActive = 1
ORDER BY l.Id;";

            var lines = await Connection.QueryAsync<CreditNoteLineDTO>(lineSql, new { Id = id });
            head.Lines = lines.ToList();
            return head;
        }

        // ===================== CREATE =====================
        public async Task<int> CreateAsync(CreditNote cn)
        {
            // (A) ItemWarehouseStock.Available upsert by (ItemId, WarehouseId, BinId)
            const string adjustWarehouseAvailableSql = @"
UPDATE S
   SET S.Available = ISNULL(S.Available,0) + @Delta
FROM dbo.ItemWarehouseStock S
WHERE S.ItemId      = @ItemId
  AND S.WarehouseId = @WarehouseId
  AND ( (S.BinId = @BinId) OR (S.BinId IS NULL AND @BinId IS NULL) );

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.ItemWarehouseStock
        (ItemId, WarehouseId, BinId, OnHand, Reserved, MinQty, MaxQty, ReorderQty, Available)
    VALUES
        (@ItemId, @WarehouseId, @BinId, 0, 0, 0, 0, 0, @Delta);
END;";

            // (B1) ItemPrice: increment Qty (RESTOCK/EXCESS) by (ItemId,SupplierId,WarehouseId)
            const string adjustSupplierQtySql = @"
;WITH ip AS (
    SELECT TOP(1) *
    FROM dbo.ItemPrice
    WHERE ItemId=@ItemId AND SupplierId=@SupplierId AND ISNULL(WarehouseId,0)=ISNULL(@WarehouseId,0)
    ORDER BY Id DESC
)
UPDATE ip SET Qty = ISNULL(Qty,0) + @Delta;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.ItemPrice (ItemId, SupplierId, WarehouseId, Price, Barcode, Qty, BadCountedQty)
    VALUES (@ItemId, @SupplierId, @WarehouseId, 0, NULL, @Delta, 0);
END;";

            // (B2) ItemPrice: increment BadCountedQty (DAMAGED/SCRAP)
            const string adjustSupplierBadSql = @"
;WITH ip AS (
    SELECT TOP(1) *
    FROM dbo.ItemPrice
    WHERE ItemId=@ItemId AND SupplierId=@SupplierId AND ISNULL(WarehouseId,0)=ISNULL(@WarehouseId,0)
    ORDER BY Id DESC
)
UPDATE ip SET BadCountedQty = ISNULL(BadCountedQty,0) + @Delta;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.ItemPrice (ItemId, SupplierId, WarehouseId, Price, Barcode, Qty, BadCountedQty)
    VALUES (@ItemId, @SupplierId, @WarehouseId, 0, NULL, 0, @Delta);
END;";

            if (cn is null) throw new ArgumentNullException(nameof(cn));

            var now = DateTime.UtcNow;
            if (cn.CreatedDate == default) cn.CreatedDate = now;
            if (cn.UpdatedDate == null) cn.UpdatedDate = now;

            const string doInfoSql = @"
SELECT
    d.Id               AS DoId,
    d.DoNumber,
    soctx.SoId,
    soctx.CustomerId,
    soctx.CustomerName,
    sictx.SiId,
    sictx.SiNumber
FROM dbo.DeliveryOrder d
OUTER APPLY (
    SELECT TOP (1)
        so.Id          AS SoId,
        so.CustomerId  AS CustomerId,
        c.CustomerName AS CustomerName
    FROM dbo.DeliveryOrderLine dl
    JOIN dbo.SalesOrderLines  sol ON sol.Id = dl.SoLineId
    JOIN dbo.SalesOrder       so  ON so.Id = sol.SalesOrderId
    LEFT JOIN dbo.Customer    c   ON c.Id = so.CustomerId
    WHERE dl.DoId = d.Id
    ORDER BY dl.Id
) AS soctx
OUTER APPLY (
    SELECT TOP (1)
        s.Id        AS SiId,
        s.InvoiceNo AS SiNumber
    FROM dbo.SalesInvoice s
    WHERE s.IsActive = 1 AND s.SourceType = 2 AND s.DoId = d.Id
    ORDER BY s.Id DESC
) AS sictx
WHERE d.Id = @DoId;";

            const string existsSiSql = @"
SELECT TOP (1) 1
FROM dbo.CreditNote
WHERE IsActive = 1 AND SiId = @SiId;";

            const string insertHdr = @"
INSERT INTO dbo.CreditNote
(
    CreditNoteNo, DoId, DoNumber, SiId, SiNumber,
    CustomerId, CustomerName, CreditNoteDate,
    Status, Subtotal,
    CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive
)
OUTPUT INSERTED.Id
VALUES
(
    @CreditNoteNo, @DoId, @DoNumber, @SiId, @SiNumber,
    @CustomerId, @CustomerName, @CreditNoteDate,
    @Status, @Subtotal,
    @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, 1
);";

            const string insertLine = @"
INSERT INTO dbo.CreditNoteLine
(
    CreditNoteId, DoId, SiId, DoLineId,
    ItemId, ItemName, Uom,
    DeliveredQty, ReturnedQty,
    UnitPrice, DiscountPct,GstPct,Tax,TaxCodeId,
    LineNet, ReasonId, RestockDispositionId,
    WarehouseId, SupplierId, BinId, IsActive
)
OUTPUT INSERTED.Id
VALUES
(
    @CreditNoteId, @DoId, @SiId,@DoLineId,
    @ItemId, @ItemName, @Uom,
    @DeliveredQty, @ReturnedQty,
    @UnitPrice, @DiscountPct, @GstPct,@Tax,@TaxCodeId,
    @LineNet, @ReasonId, @RestockDispositionId,
    @WarehouseId, @SupplierId, @BinId, 1
);";

            const string nextNo = @"
DECLARE @n INT;
SELECT @n = ISNULL(MAX(TRY_CONVERT(int, RIGHT(CreditNoteNo, 6))), 0) + 1
FROM dbo.CreditNote WITH (UPDLOCK, HOLDLOCK);
SELECT CONCAT('CN-', RIGHT(CONCAT(REPLICATE('0', 6), @n), 6));";

            var conn = Connection;
            if (conn.State != ConnectionState.Open)
                await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();
            try
            {
                // 1) enrich header from DO
                var info = await conn.QueryFirstOrDefaultAsync(
                    new CommandDefinition(doInfoSql, new { DoId = cn.DoId }, transaction: tx));
                if (info is null) throw new InvalidOperationException("Delivery Order not found.");

                cn.DoNumber ??= (string?)info.DoNumber;
                cn.SiId = cn.SiId != 0 ? cn.SiId : (int?)info.SiId ?? 0;
                cn.SiNumber ??= (string?)info.SiNumber;
                cn.CustomerId = cn.CustomerId != 0 ? cn.CustomerId : (int?)info.CustomerId ?? 0;
                cn.CustomerName ??= (string?)info.CustomerName ?? "";

                // 1a) only one CN per Sales Invoice
                if (cn.SiId > 0)
                {
                    var exists = await conn.ExecuteScalarAsync<int?>(
                        existsSiSql,
                        new { SiId = cn.SiId },
                        tx);

                    if (exists.HasValue)
                        throw new InvalidOperationException("A credit note already exists for this Sales Invoice.");
                }

                // 2) numbering
                cn.CreditNoteNo = await conn.ExecuteScalarAsync<string>(nextNo, transaction: tx);

                // 3) GST / LineNet calculation per line (TaxCodeId NOT used in condition)
                foreach (var l in cn.Lines ?? Enumerable.Empty<CreditNoteLine>())
                {
                    var rawBase = l.ReturnedQty * l.UnitPrice * (1 - (l.DiscountPct / 100m));
                    var baseAmount = decimal.Round(rawBase, 2, MidpointRounding.AwayFromZero);

                    var gstPct = l.GstPct;
                    var taxFlag = (l.Tax ?? "").ToUpperInvariant(); // EXCLUSIVE / INCLUSIVE / EXEMPT

                    decimal gstAmount;
                    decimal lineNet;

                    if (taxFlag == "EXEMPT" || gstPct <= 0m)
                    {
                        // EXEMPT / 0% GST
                        gstAmount = 0m;
                        lineNet = baseAmount;
                    }
                    else if (taxFlag == "STANDARD-RATED")
                    {
                        // net price, GST on top
                        gstAmount = decimal.Round(baseAmount * (gstPct / 100m),
                                                  2, MidpointRounding.AwayFromZero);
                        lineNet = baseAmount + gstAmount;
                    }
                    else
                    {
                        // INCLUSIVE (or unknown treated as inclusive): baseAmount already includes GST
                        var divisor = 1 + (gstPct / 100m);
                        var netPortion = divisor == 0m
                            ? baseAmount
                            : decimal.Round(baseAmount / divisor, 2, MidpointRounding.AwayFromZero);
                        gstAmount = baseAmount - netPortion; // info only (not stored)
                        lineNet = baseAmount;
                    }

                    l.LineNet = lineNet;
                }

                cn.Subtotal = decimal.Round(
                    (cn.Lines ?? Enumerable.Empty<CreditNoteLine>()).Sum(x => x.LineNet),
                    2,
                    MidpointRounding.AwayFromZero);

                // 4) insert header
                var newId = await conn.ExecuteScalarAsync<int>(insertHdr, new
                {
                    cn.CreditNoteNo,
                    cn.DoId,
                    cn.DoNumber,
                    SiId = cn.SiId,
                    cn.SiNumber,
                    cn.CustomerId,
                    cn.CustomerName,
                    cn.CreditNoteDate,
                    Status = (byte)cn.Status,
                    cn.Subtotal,
                    cn.CreatedBy,
                    cn.CreatedDate,
                    cn.UpdatedBy,
                    UpdatedDate = cn.UpdatedDate ?? now
                }, tx);

                // 5) insert lines
                var approved = (byte)cn.Status == 2;
                foreach (var l in cn.Lines ?? Enumerable.Empty<CreditNoteLine>())
                {
                    var lineId = await conn.ExecuteScalarAsync<int>(insertLine, new
                    {
                        CreditNoteId = newId,
                        DoId = (int?)cn.DoId,
                        SiId = (int?)cn.SiId,
                        l.DoLineId,
                        l.ItemId,
                        l.ItemName,
                        Uom = l.Uom,
                        l.DeliveredQty,
                        l.ReturnedQty,
                        l.UnitPrice,
                        l.DiscountPct,
                        l.GstPct,
                        l.Tax,
                        l.TaxCodeId,
                        l.LineNet,
                        l.ReasonId,
                        l.RestockDispositionId,
                        l.WarehouseId,
                        l.SupplierId,
                        l.BinId
                    }, tx);

                    // 6) STOCK EFFECTS only if Approved at create-time
                    if (!approved) continue;

                    var qty = Math.Max(0m, l.ReturnedQty);
                    var w = l.WarehouseId ?? 0;
                    var s = l.SupplierId ?? 0;
                    var b = l.BinId;

                    if (l.RestockDispositionId == 1) // RESTOCK/EXCESS
                    {
                        await conn.ExecuteAsync(adjustWarehouseAvailableSql, new { ItemId = l.ItemId, WarehouseId = w, BinId = b, Delta = qty }, tx);
                        await conn.ExecuteAsync(adjustSupplierQtySql, new { ItemId = l.ItemId, SupplierId = s, WarehouseId = w, Delta = qty }, tx);
                    }
                    else if (l.RestockDispositionId == 2) // DAMAGED/SCRAP
                    {
                        await conn.ExecuteAsync(adjustSupplierBadSql, new { ItemId = l.ItemId, SupplierId = s, WarehouseId = w, Delta = qty }, tx);
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

        // ===================== UPDATE =====================
        public async Task UpdateAsync(CreditNote cn)
        {
            // (A) ItemWarehouseStock.Available upsert by (ItemId, WarehouseId, BinId)
            const string adjustWarehouseAvailableSql = @"
UPDATE S
   SET S.Available = ISNULL(S.Available,0) + @Delta
FROM dbo.ItemWarehouseStock S
WHERE S.ItemId      = @ItemId
  AND S.WarehouseId = @WarehouseId
  AND ( (S.BinId = @BinId) OR (S.BinId IS NULL AND @BinId IS NULL) );

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.ItemWarehouseStock
        (ItemId, WarehouseId, BinId, OnHand, Reserved, MinQty, MaxQty, ReorderQty, Available)
    VALUES
        (@ItemId, @WarehouseId, @BinId, 0, 0, 0, 0, 0, @Delta);
END;";

            // (B1) ItemPrice: increment Qty (RESTOCK/EXCESS) by (ItemId,SupplierId,WarehouseId)
            const string adjustSupplierQtySql = @"
;WITH ip AS (
    SELECT TOP(1) *
    FROM dbo.ItemPrice
    WHERE ItemId=@ItemId AND SupplierId=@SupplierId AND ISNULL(WarehouseId,0)=ISNULL(@WarehouseId,0)
    ORDER BY Id DESC
)
UPDATE ip SET Qty = ISNULL(Qty,0) + @Delta;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.ItemPrice (ItemId, SupplierId, WarehouseId, Price, Barcode, Qty, BadCountedQty)
    VALUES (@ItemId, @SupplierId, @WarehouseId, 0, NULL, @Delta, 0);
END;";

            // (B2) ItemPrice: increment BadCountedQty (DAMAGED/SCRAP)
            const string adjustSupplierBadSql = @"
;WITH ip AS (
    SELECT TOP(1) *
    FROM dbo.ItemPrice
    WHERE ItemId=@ItemId AND SupplierId=@SupplierId AND ISNULL(WarehouseId,0)=ISNULL(@WarehouseId,0)
    ORDER BY Id DESC
)
UPDATE ip SET BadCountedQty = ISNULL(BadCountedQty,0) + @Delta;

IF @@ROWCOUNT = 0
BEGIN
    INSERT INTO dbo.ItemPrice (ItemId, SupplierId, WarehouseId, Price, Barcode, Qty, BadCountedQty)
    VALUES (@ItemId, @SupplierId, @WarehouseId, 0, NULL, 0, @Delta);
END;";

            if (cn is null) throw new ArgumentNullException(nameof(cn));
            var now = DateTime.UtcNow;

            const string getOldHdr = @"SELECT CAST(Status AS tinyint) AS Status FROM dbo.CreditNote WHERE Id=@Id AND IsActive=1;";
            const string getOldLines = @"
SELECT Id, ItemId, ReturnedQty, RestockDispositionId, WarehouseId, SupplierId, BinId
FROM dbo.CreditNoteLine
WHERE CreditNoteId=@Id AND IsActive=1;";

            const string updHdr = @"
UPDATE dbo.CreditNote
SET DoId=@DoId, DoNumber=@DoNumber,
    SiId=@SiId, SiNumber=@SiNumber,
    CustomerId=@CustomerId, CustomerName=@CustomerName,
    CreditNoteDate=@CreditNoteDate,
    Status=@Status, Subtotal=@Subtotal,
    UpdatedBy=@UpdatedBy, UpdatedDate=@UpdatedDate
WHERE Id=@Id AND IsActive=1;";

            const string updLine = @"
UPDATE dbo.CreditNoteLine
SET DoId=@DoId, SiId=@SiId,DoLineId = @DoLineId,
    ItemId=@ItemId, ItemName=@ItemName, Uom=@Uom,
    DeliveredQty=@DeliveredQty, ReturnedQty=@ReturnedQty,
    UnitPrice=@UnitPrice, DiscountPct=@DiscountPct,GstPct=@GstPct,Tax=@Tax, TaxCodeId=@TaxCodeId,
    LineNet=@LineNet, ReasonId=@ReasonId, RestockDispositionId=@RestockDispositionId,
    WarehouseId=@WarehouseId, SupplierId=@SupplierId, BinId=@BinId,
    IsActive=1
WHERE Id=@Id AND CreditNoteId=@CreditNoteId;";

            const string insLine = @"
INSERT INTO dbo.CreditNoteLine
(
    CreditNoteId, DoId, SiId,DoLineId,
    ItemId, ItemName, Uom,
    DeliveredQty, ReturnedQty,
    UnitPrice, DiscountPct,GstPct,Tax, TaxCodeId,
    LineNet, ReasonId, RestockDispositionId,
    WarehouseId, SupplierId, BinId, IsActive
)
OUTPUT INSERTED.Id
VALUES
(
    @CreditNoteId, @DoId, @SiId,@DoLineId,
    @ItemId, @ItemName, @Uom,
    @DeliveredQty, @ReturnedQty,
    @UnitPrice, @DiscountPct,@GstPct,@Tax,@TaxCodeId,
    @LineNet, @ReasonId, @RestockDispositionId,
    @WarehouseId, @SupplierId, @BinId, 1
);";

            const string softDeleteMissing = @"
UPDATE dbo.CreditNoteLine
SET IsActive=0
WHERE CreditNoteId=@CreditNoteId AND IsActive=1
  AND (@KeepIdsCount=0 OR Id NOT IN @KeepIds);";

            // --- recompute GST + totals (TaxCodeId NOT used in condition) ---
            foreach (var l in cn.Lines ?? Enumerable.Empty<CreditNoteLine>())
            {
                var rawBase = l.ReturnedQty * l.UnitPrice * (1 - (l.DiscountPct / 100m));
                var baseAmount = decimal.Round(rawBase, 2, MidpointRounding.AwayFromZero);

                var gstPct = l.GstPct;
                var taxFlag = (l.Tax ?? "").ToUpperInvariant();

                decimal gstAmount;
                decimal lineNet;

                if (taxFlag == "EXEMPT" || gstPct <= 0m)
                {
                    gstAmount = 0m;
                    lineNet = baseAmount;
                }
                else if (taxFlag == "STANDARD-RATED")
                {
                    gstAmount = decimal.Round(baseAmount * (gstPct / 100m),
                                              2, MidpointRounding.AwayFromZero);
                    lineNet = baseAmount + gstAmount;
                }
                else
                {
                    var divisor = 1 + (gstPct / 100m);
                    var netPortion = divisor == 0m
                        ? baseAmount
                        : decimal.Round(baseAmount / divisor, 2, MidpointRounding.AwayFromZero);
                    gstAmount = baseAmount - netPortion;
                    lineNet = baseAmount;
                }

                l.LineNet = lineNet;
            }

            cn.Subtotal = decimal.Round(
                (cn.Lines ?? Enumerable.Empty<CreditNoteLine>()).Sum(x => x.LineNet),
                2,
                MidpointRounding.AwayFromZero);

            var conn = Connection;
            if (conn.State != ConnectionState.Open) await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();
            try
            {
                var oldStatus = await conn.ExecuteScalarAsync<byte?>(getOldHdr, new { cn.Id }, tx) ?? 1;
                var newStatus = (byte)cn.Status;

                var oldLines = (await conn.QueryAsync(getOldLines, new { Id = cn.Id }, tx)).ToList();
                var oldMap = oldLines.ToDictionary(x => (int)x.Id);

                // 1) Approved -> Draft: revert all old effects first
                if (oldStatus == 2 && newStatus == 1)
                {
                    foreach (var d in oldLines)
                    {
                        var qty = Math.Max(0m, (decimal)d.ReturnedQty);
                        var w = (int?)d.WarehouseId ?? 0;
                        var s = (int?)d.SupplierId ?? 0;
                        var b = (int?)d.BinId;

                        if ((int)d.RestockDispositionId == 1)
                        {
                            await conn.ExecuteAsync(adjustWarehouseAvailableSql, new { ItemId = (int)d.ItemId, WarehouseId = w, BinId = b, Delta = -qty }, tx);
                            await conn.ExecuteAsync(adjustSupplierQtySql, new { ItemId = (int)d.ItemId, SupplierId = s, WarehouseId = w, Delta = -qty }, tx);
                        }
                        else if ((int)d.RestockDispositionId == 2)
                        {
                            await conn.ExecuteAsync(adjustSupplierBadSql, new { ItemId = (int)d.ItemId, SupplierId = s, WarehouseId = w, Delta = -qty }, tx);
                        }
                    }
                }

                // 2) Update header
                await conn.ExecuteAsync(updHdr, new
                {
                    cn.DoId,
                    cn.DoNumber,
                    cn.SiId,
                    cn.SiNumber,
                    cn.CustomerId,
                    cn.CustomerName,
                    cn.CreditNoteDate,
                    Status = newStatus,
                    cn.Subtotal,
                    cn.UpdatedBy,
                    UpdatedDate = now,
                    cn.Id
                }, tx);

                // 3) Upsert lines
                var keepIds = new List<int>();
                foreach (var l in cn.Lines ?? Enumerable.Empty<CreditNoteLine>())
                {
                    if (l.Id > 0)
                    {
                        await conn.ExecuteAsync(updLine, new
                        {
                            l.Id,
                            CreditNoteId = cn.Id,
                            DoId = (int?)cn.DoId,
                            SiId = (int?)cn.SiId,
                            l.DoLineId,
                            l.ItemId,
                            l.ItemName,
                            Uom = l.Uom,
                            l.DeliveredQty,
                            l.ReturnedQty,
                            l.UnitPrice,
                            l.DiscountPct,
                            l.GstPct,
                            l.Tax,
                            l.TaxCodeId,
                            l.LineNet,
                            l.ReasonId,
                            l.RestockDispositionId,
                            l.WarehouseId,
                            l.SupplierId,
                            l.BinId
                        }, tx);
                        keepIds.Add(l.Id);
                    }
                    else
                    {
                        var newLineId = await conn.ExecuteScalarAsync<int>(insLine, new
                        {
                            CreditNoteId = cn.Id,
                            DoId = (int?)cn.DoId,
                            SiId = (int?)cn.SiId,
                            l.DoLineId,
                            l.ItemId,
                            l.ItemName,
                            Uom = l.Uom,
                            l.DeliveredQty,
                            l.ReturnedQty,
                            l.UnitPrice,
                            l.DiscountPct,
                            l.GstPct,
                            l.Tax,
                            l.TaxCodeId,
                            l.LineNet,
                            l.ReasonId,
                            l.RestockDispositionId,
                            l.WarehouseId,
                            l.SupplierId,
                            l.BinId
                        }, tx);
                        keepIds.Add(newLineId);
                    }
                }

                // 4) Soft-delete removed lines
                await conn.ExecuteAsync(softDeleteMissing, new
                {
                    CreditNoteId = cn.Id,
                    KeepIds = keepIds.Count == 0 ? new[] { -1 } : keepIds.ToArray(),
                    KeepIdsCount = keepIds.Count
                }, tx);

                // 5) STOCK EFFECTS if Approved
                if (newStatus == 2)
                {
                    if (oldStatus != 2)
                    {
                        // Draft -> Approved: apply full effects
                        var curLines = await conn.QueryAsync(getOldLines, new { Id = cn.Id }, tx);
                        foreach (var l in curLines)
                        {
                            var qty = Math.Max(0m, (decimal)l.ReturnedQty);
                            var w = (int?)l.WarehouseId ?? 0;
                            var s = (int?)l.SupplierId ?? 0;
                            var b = (int?)l.BinId;

                            if ((int)l.RestockDispositionId == 1)
                            {
                                await conn.ExecuteAsync(adjustWarehouseAvailableSql, new { ItemId = (int)l.ItemId, WarehouseId = w, BinId = b, Delta = qty }, tx);
                                await conn.ExecuteAsync(adjustSupplierQtySql, new { ItemId = (int)l.ItemId, SupplierId = s, WarehouseId = w, Delta = qty }, tx);
                            }
                            else if ((int)l.RestockDispositionId == 2)
                            {
                                await conn.ExecuteAsync(adjustSupplierBadSql, new { ItemId = (int)l.ItemId, SupplierId = s, WarehouseId = w, Delta = qty }, tx);
                            }
                        }
                    }
                    else
                    {
                        // Approved -> Approved: apply deltas vs oldMap
                        var curLines = await conn.QueryAsync(getOldLines, new { Id = cn.Id }, tx);
                        var curMap = curLines.ToDictionary(x => (int)x.Id);

                        // Changes / additions
                        foreach (var l in curLines)
                        {
                            if (!oldMap.TryGetValue((int)l.Id, out var old))
                            {
                                var qty = Math.Max(0m, (decimal)l.ReturnedQty);
                                var w = (int?)l.WarehouseId ?? 0;
                                var s = (int?)l.SupplierId ?? 0;
                                var b = (int?)l.BinId;

                                if ((int)l.RestockDispositionId == 1)
                                {
                                    await conn.ExecuteAsync(adjustWarehouseAvailableSql, new { ItemId = (int)l.ItemId, WarehouseId = w, BinId = b, Delta = qty }, tx);
                                    await conn.ExecuteAsync(adjustSupplierQtySql, new { ItemId = (int)l.ItemId, SupplierId = s, WarehouseId = w, Delta = qty }, tx);
                                }
                                else if ((int)l.RestockDispositionId == 2)
                                {
                                    await conn.ExecuteAsync(adjustSupplierBadSql, new { ItemId = (int)l.ItemId, SupplierId = s, WarehouseId = w, Delta = qty }, tx);
                                }
                                continue;
                            }

                            bool keyChanged =
                                (int?)old.WarehouseId != (int?)l.WarehouseId ||
                                (int?)old.SupplierId != (int?)l.SupplierId ||
                                (int?)old.BinId != (int?)l.BinId ||
                                (int)old.RestockDispositionId != (int)l.RestockDispositionId ||
                                (int)old.ItemId != (int)l.ItemId;

                            var oldQty = Math.Max(0m, (decimal)old.ReturnedQty);
                            var newQty = Math.Max(0m, (decimal)l.ReturnedQty);

                            if (keyChanged)
                            {
                                var ow = (int?)old.WarehouseId ?? 0;
                                var os = (int?)old.SupplierId ?? 0;
                                var ob = (int?)old.BinId;

                                if ((int)old.RestockDispositionId == 1)
                                {
                                    await conn.ExecuteAsync(adjustWarehouseAvailableSql, new { ItemId = (int)old.ItemId, WarehouseId = ow, BinId = ob, Delta = -oldQty }, tx);
                                    await conn.ExecuteAsync(adjustSupplierQtySql, new { ItemId = (int)old.ItemId, SupplierId = os, WarehouseId = ow, Delta = -oldQty }, tx);
                                }
                                else if ((int)old.RestockDispositionId == 2)
                                {
                                    await conn.ExecuteAsync(adjustSupplierBadSql, new { ItemId = (int)old.ItemId, SupplierId = os, WarehouseId = ow, Delta = -oldQty }, tx);
                                }

                                var w = (int?)l.WarehouseId ?? 0;
                                var s = (int?)l.SupplierId ?? 0;
                                var b = (int?)l.BinId;

                                if ((int)l.RestockDispositionId == 1)
                                {
                                    await conn.ExecuteAsync(adjustWarehouseAvailableSql, new { ItemId = (int)l.ItemId, WarehouseId = w, BinId = b, Delta = newQty }, tx);
                                    await conn.ExecuteAsync(adjustSupplierQtySql, new { ItemId = (int)l.ItemId, SupplierId = s, WarehouseId = w, Delta = newQty }, tx);
                                }
                                else if ((int)l.RestockDispositionId == 2)
                                {
                                    await conn.ExecuteAsync(adjustSupplierBadSql, new { ItemId = (int)l.ItemId, SupplierId = s, WarehouseId = w, Delta = newQty }, tx);
                                }
                            }
                            else
                            {
                                var delta = newQty - oldQty;
                                if (delta == 0m) continue;

                                var w = (int?)l.WarehouseId ?? 0;
                                var s = (int?)l.SupplierId ?? 0;
                                var b = (int?)l.BinId;

                                if ((int)l.RestockDispositionId == 1)
                                {
                                    await conn.ExecuteAsync(adjustWarehouseAvailableSql, new { ItemId = (int)l.ItemId, WarehouseId = w, BinId = b, Delta = delta }, tx);
                                    await conn.ExecuteAsync(adjustSupplierQtySql, new { ItemId = (int)l.ItemId, SupplierId = s, WarehouseId = w, Delta = delta }, tx);
                                }
                                else if ((int)l.RestockDispositionId == 2)
                                {
                                    await conn.ExecuteAsync(adjustSupplierBadSql, new { ItemId = (int)l.ItemId, SupplierId = s, WarehouseId = w, Delta = delta }, tx);
                                }
                            }
                        }

                        // Deletions: revert deleted lines
                        var curIds = new HashSet<int>(curMap.Keys);
                        var deleted = oldLines.Where(o => !curIds.Contains((int)o.Id));
                        foreach (var d in deleted)
                        {
                            var qty = Math.Max(0m, (decimal)d.ReturnedQty);
                            var w = (int?)d.WarehouseId ?? 0;
                            var s = (int?)d.SupplierId ?? 0;
                            var b = (int?)d.BinId;

                            if ((int)d.RestockDispositionId == 1)
                            {
                                await conn.ExecuteAsync(adjustWarehouseAvailableSql, new { ItemId = (int)d.ItemId, WarehouseId = w, BinId = b, Delta = -qty }, tx);
                                await conn.ExecuteAsync(adjustSupplierQtySql, new { ItemId = (int)d.ItemId, SupplierId = s, WarehouseId = w, Delta = -qty }, tx);
                            }
                            else if ((int)d.RestockDispositionId == 2)
                            {
                                await conn.ExecuteAsync(adjustSupplierBadSql, new { ItemId = (int)d.ItemId, SupplierId = s, WarehouseId = w, Delta = -qty }, tx);
                            }
                        }
                    }
                }

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ===================== SOFT DELETE =====================
        public async Task DeactivateAsync(int id, int updatedBy)
        {
            const string sqlHeader = @"
UPDATE dbo.CreditNote
SET IsActive=0, UpdatedBy=@UpdatedBy, UpdatedDate=SYSUTCDATETIME()
WHERE Id=@Id AND IsActive=1;";

            const string sqlLines = @"
UPDATE dbo.CreditNoteLine
SET IsActive=0
WHERE CreditNoteId=@Id AND IsActive=1;";

            var conn = Connection;
            if (conn.State != ConnectionState.Open) await (conn as SqlConnection)!.OpenAsync();

            using var tx = conn.BeginTransaction();
            try
            {
                var n = await conn.ExecuteAsync(sqlHeader, new { Id = id, UpdatedBy = updatedBy }, tx);
                if (n == 0) throw new KeyNotFoundException("Credit note not found.");
                await conn.ExecuteAsync(sqlLines, new { Id = id }, tx);
                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        // ===================== DO → SI LINE ENRICH =====================
        // Simplified: no remaining qty logic, because only 1 CN allowed per SI.
        public async Task<IEnumerable<object>> GetDoLinesAsync(int doId, int? excludeCnId = null)
        {
            const string sql = @"
SELECT 
    dl.Id                      AS DoLineId,
    dl.ItemId,
    i.ItemName,
    dl.Uom,
    dl.WarehouseId,
    dl.BinId,
    dl.SupplierId,
    CAST(dl.Qty AS decimal(18,4)) AS QtyDelivered,
    CAST(dl.Qty AS decimal(18,4)) AS QtyRemaining,   -- UI can treat this as delivered
    COALESCE(si.UnitPrice, 0)   AS UnitPrice,
    COALESCE(si.DiscountPct, 0) AS DiscountPct,
    si.TaxCodeId,
    si.GstPct,
    si.Tax
FROM dbo.DeliveryOrderLine dl
JOIN dbo.DeliveryOrder     d  ON d.Id = dl.DoId
LEFT JOIN dbo.Item         i  ON i.Id = dl.ItemId
OUTER APPLY (
    SELECT TOP (1) sil.UnitPrice, sil.DiscountPct, sil.TaxCodeId, sil.GstPct, sil.Tax
    FROM dbo.SalesInvoiceLine sil
    WHERE (sil.SourceType = 2 AND sil.SourceLineId = dl.Id)
       OR (sil.SourceType = 1 AND sil.SourceLineId = dl.SoLineId)
       OR (sil.ItemId = dl.ItemId)
    ORDER BY
        CASE 
          WHEN (sil.SourceType = 2 AND sil.SourceLineId = dl.Id) THEN 0
          WHEN (sil.SourceType = 1 AND sil.SourceLineId = dl.SoLineId) THEN 1
          ELSE 2
        END,
        sil.Id DESC
) si
WHERE dl.DoId = @DoId
ORDER BY dl.Id;";

            // excludeCnId no longer used (only one CN per SI), but kept in signature for compatibility
            return await Connection.QueryAsync(sql, new { DoId = doId });
        }
    }
}
