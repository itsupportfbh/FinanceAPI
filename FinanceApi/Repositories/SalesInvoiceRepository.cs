using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using static FinanceApi.ModelDTO.SalesInvoiceDtos;

namespace FinanceApi.Repositories
{
    public class SalesInvoiceRepository : DynamicRepository, ISalesInvoiceRepository
    {
        public SalesInvoiceRepository(IDbConnectionFactory factory) : base(factory) { }

        public async Task<IEnumerable<SiSourceLineDto>> GetSourceLinesAsync(byte sourceType, int sourceId)
        {
            var sql = sourceType == 1
                ? @"SELECT SourceLineId, SourceType, SourceId,
                           ItemId, ItemName, UomName,
                           QtyOpen, UnitPrice, DiscountPct, GstPct, Tax
                    FROM dbo.vw_SI_SourceFromSO
                    WHERE SourceId=@Id"
                : @"SELECT SourceLineId, SourceType, SourceId,
                           ItemId, ItemName, UomName,
                           QtyOpen, UnitPrice, DiscountPct, GstPct, Tax
                    FROM dbo.vw_SI_SourceFromDO
                    WHERE SourceId=@Id";

            return await Connection.QueryAsync<SiSourceLineDto>(sql, new { Id = sourceId });
        }

        public async Task<int> CreateAsync(int userId, SiCreateRequest req)
        {
            // 1) Insert SalesInvoice header
            const string insHead = @"
INSERT INTO dbo.SalesInvoice
(
    InvoiceDate, 
    SourceType, 
    SoId, 
    DoId,
    Subtotal, 
    ShippingCost, 
    TaxAmount,
    Total,
    Status, 
    CreatedBy, 
    CreatedDate, 
    UpdatedBy, 
    UpdatedDate, 
    IsActive, 
    Remarks
)
OUTPUT INSERTED.Id
VALUES
(
    @InvoiceDate, 
    @SourceType, 
    @SoId, 
    @DoId,
    @Subtotal, 
    @ShippingCost, 
    @TaxAmount,
    @Total,
    0, 
    @UserId, 
    SYSUTCDATETIME(), 
    @UserId, 
    SYSUTCDATETIME(), 
    1, 
    @Remarks
);";

            var siId = await Connection.ExecuteScalarAsync<int>(
                insHead,
                new
                {
                    req.InvoiceDate,
                    req.SourceType,
                    SoId = req.SoId,
                    DoId = req.DoId,
                    Subtotal = req.Subtotal,
                    ShippingCost = req.ShippingCost,
                    TaxAmount = req.TaxAmount,   // 🔹 NEW
                    Total = req.Total,
                    Remarks = req.Remarks,
                    UserId = userId
                }
            );

            // 2) Insert SalesInvoice lines
            const string insLine = @"
INSERT INTO dbo.SalesInvoiceLine
(
    SiId, 
    SourceType, 
    SourceLineId,
    ItemId, 
    ItemName, 
    Uom,
    Qty, 
    UnitPrice, 
    DiscountPct, 
    GstPct, 
    Tax,
    TaxCodeId,
    LineAmount,
    TaxAmount,
    Description,
    BudgetLineId
)
VALUES
(
    @SiId, 
    @SourceType, 
    @SourceLineId,
    @ItemId, 
    @ItemName, 
    @Uom,
    @Qty, 
    @UnitPrice, 
    @DiscountPct, 
    @GstPct, 
    @Tax,
    @TaxCodeId,
    @LineAmount,
    @TaxAmount,
    @Description,
    @BudgetLineId
);";

            foreach (var l in req.Lines)
            {
                await Connection.ExecuteAsync(
                    insLine,
                    new
                    {
                        SiId = siId,
                        SourceType = req.SourceType,
                        SourceLineId = l.SourceLineId,
                        l.ItemId,
                        l.ItemName,
                        Uom = l.Uom,
                        l.Qty,
                        l.UnitPrice,
                        l.DiscountPct,
                        l.GstPct,
                        l.Tax,
                        l.TaxCodeId,
                        l.LineAmount,
                        TaxAmount = l.TaxAmount,   // 🔹 NEW
                        Description = string.IsNullOrWhiteSpace(l.Description)
                            ? l.ItemName
                            : l.Description,
                        l.BudgetLineId
                    }
                );
            }

            // 3) Recalculate totals
            await RecalculateTotalAsync(siId);

            // 4) Set InvoiceNo (SI-000001 style)
            await Connection.ExecuteAsync(
                @"UPDATE dbo.SalesInvoice
                  SET InvoiceNo = CONCAT('SI-', RIGHT(CONVERT(VARCHAR(8), Id + 100000), 6))
                  WHERE Id = @Id;",
                new { Id = siId }
            );

            // 5️⃣ Advance logic (unchanged)
            if (req.AdvanceId.HasValue &&
                req.AdvanceApplyAmount.HasValue &&
                req.AdvanceApplyAmount.Value > 0)
            {
                const string advanceSql = @"
IF EXISTS (
    SELECT 1
    FROM dbo.ArCustomerAdvance
    WHERE Id = @AdvanceId
      AND BalanceAmount >= @ApplyAmount
)
BEGIN
    INSERT INTO dbo.ArCustomerAdvanceAdjust
    (
        AdvanceId,
        SalesInvoiceId,
        AdjustAmount,
        AdjustDate,
        CreatedDate
    )
    VALUES
    (
        @AdvanceId,
        @SiId,
        @ApplyAmount,
        @InvoiceDate,
        SYSUTCDATETIME()
    );

    UPDATE dbo.ArCustomerAdvance
    SET BalanceAmount = BalanceAmount - @ApplyAmount
    WHERE Id = @AdvanceId;
END
ELSE
BEGIN
    THROW 50001, 'Advance balance is insufficient.', 1;
END;";

                await Connection.ExecuteAsync(
                    advanceSql,
                    new
                    {
                        AdvanceId = req.AdvanceId.Value,
                        ApplyAmount = req.AdvanceApplyAmount.Value,
                        SiId = siId,
                        req.InvoiceDate
                    }
                );
            }

            // 6) POST TO GlTransaction (unchanged logic except small fix: no + GstPct)
            const string glInsertSql = @"
INSERT INTO dbo.GlTransaction
(
    AccountId,
    TxnDate,
    CurrencyId,
    AmountFC,
    AmountBase
)
-- a) AR line (Customer BudgetLineId)
SELECT
    c.BudgetLineId              AS AccountId,
    si.InvoiceDate              AS TxnDate,
    1                           AS CurrencyId,
    si.Total                    AS AmountFC,
    si.Total                    AS AmountBase       -- DR AR
FROM dbo.SalesInvoice si
INNER JOIN dbo.SalesOrder so ON so.Id = si.SoId
INNER JOIN dbo.Customer    c  ON c.Id = so.CustomerId
WHERE si.Id = @SiId
  AND c.BudgetLineId IS NOT NULL

UNION ALL

-- b) Revenue lines (Item BudgetLineId)
SELECT
    i.BudgetLineId              AS AccountId,
    si.InvoiceDate              AS TxnDate,
    1                           AS CurrencyId,
    sil.LineAmount              AS AmountFC,
    -sil.LineAmount             AS AmountBase       -- CR Income
FROM dbo.SalesInvoiceLine sil
INNER JOIN dbo.SalesInvoice si ON si.Id = sil.SiId
INNER JOIN dbo.Item         i   ON i.Id = sil.ItemId
WHERE sil.SiId = @SiId
  AND i.BudgetLineId IS NOT NULL;";

            await Connection.ExecuteAsync(glInsertSql, new { SiId = siId });

            return siId;
        }

        public async Task<SiHeaderDto?> GetAsync(int id)
        {
            const string sql = @"
SELECT
    si.Id,
    ISNULL(si.InvoiceNo,'') AS InvoiceNo,
    si.InvoiceDate,
    si.SourceType,
    si.SoId,
    si.DoId,
    si.TaxAmount,
    si.Total,
    si.Status,
    si.IsActive,
    si.Remarks,

    -- SourceRef
    SourceRef =
      CASE
        WHEN si.SourceType = 1 THEN ISNULL(so.SalesOrderNo,'')
        WHEN si.SourceType = 2 THEN ISNULL(do1.DoNumber,'')
        ELSE ''
      END,

    -- Customer
    so.CustomerId,
    CustomerName = ISNULL(c.CustomerName,''),
    ContactNumber = ISNULL(c.ContactNumber,''),
    PointOfContactPerson = ISNULL(c.PointOfContactPerson,''),
    Email = ISNULL(c.Email,''),

    -- Payment Term (from Customer)
    PaymentTermId = c.PaymentTermId,

    -- Quotation (FIXED JOIN)
    q.CurrencyId,
    FxRate = ISNULL(q.FxRate, 0),
    DeliveryDate = q.DeliveryDate,
    DeliveryTo = q.DeliveryTo,
	p.PaymentTermsName,
	cu.CurrencyName
FROM dbo.SalesInvoice si
LEFT JOIN Finance.dbo.SalesOrder so     ON so.Id = si.SoId
LEFT JOIN Finance.dbo.DeliveryOrder do1 ON do1.Id = si.DoId

-- ✅ FIX HERE
LEFT JOIN dbo.Quotation q
    ON q.Id = so.QuotationNo
   AND q.IsActive = 1

LEFT JOIN dbo.Customer c
    ON c.Id = so.CustomerId
	LEFT JOIN dbo.PaymentTerms p
    ON p.Id = q.PaymentTermsId
	LEFT JOIN dbo.Currency cu
    ON cu.Id = q.CurrencyId
WHERE si.Id =@Id;

";

            return await Connection.QueryFirstOrDefaultAsync<SiHeaderDto>(sql, new { Id = id });
        }


        public async Task<IEnumerable<SiLineDto>> GetLinesAsync(int id)
        {
            const string sql = @"
SELECT
    Id, SiId, SourceType, SourceLineId,
    ItemId, ItemName, Uom,
    Qty, UnitPrice, DiscountPct, GstPct, Tax,
    TaxCodeId,
    LineAmount,
    TaxAmount,
    Description,
    BudgetLineId
FROM dbo.SalesInvoiceLine
WHERE SiId=@Id;";

            return await Connection.QueryAsync<SiLineDto>(sql, new { Id = id });
        }

        public async Task<IEnumerable<SiListRowDto>> GetListAsync()
        {
            const string sql = @"
SELECT
    si.Id,
    ISNULL(si.InvoiceNo,'') AS InvoiceNo,
    si.InvoiceDate,
    si.SourceType,
    CASE
        WHEN si.SourceType = 1 THEN ISNULL(so.SalesOrderNo,'')
        WHEN si.SourceType = 2 THEN ISNULL(d.DoNumber,'')
    END AS SourceRef,
    si.Total
FROM dbo.SalesInvoice si
LEFT JOIN dbo.SalesOrder    so ON so.Id = si.SoId
LEFT JOIN dbo.DeliveryOrder d  ON d.Id = si.DoId
WHERE si.IsActive = 1
ORDER BY si.Id DESC;";

            return await Connection.QueryAsync<SiListRowDto>(sql);
        }

        public async Task DeactivateAsync(int id)
        {
            await Connection.ExecuteAsync(
                @"UPDATE dbo.SalesInvoice
                  SET IsActive=0,
                      UpdatedDate=SYSUTCDATETIME()
                  WHERE Id=@Id;",
                new { Id = id });
        }

        public async Task UpdateHeaderAsync(int id, DateTime invoiceDate, int userId)
        {
            const string sql = @"
UPDATE dbo.SalesInvoice
SET InvoiceDate=@InvoiceDate,
    UpdatedBy=@UserId,
    UpdatedDate=SYSUTCDATETIME()
WHERE Id=@Id;";

            await Connection.ExecuteAsync(sql, new { Id = id, InvoiceDate = invoiceDate, UserId = userId });
        }

        public async Task<int> AddLineAsync(int siId, SiCreateLine l, byte sourceType)
        {
            const string sql = @"
INSERT INTO dbo.SalesInvoiceLine
(
    SiId, SourceType, SourceLineId,
    ItemId, ItemName, Uom,
    Qty, UnitPrice, DiscountPct, GstPct, Tax,
    TaxCodeId,
    LineAmount,
    TaxAmount,
    Description,
    BudgetLineId
)
OUTPUT INSERTED.Id
VALUES
(
    @SiId, @SourceType, @SourceLineId,
    @ItemId, @ItemName, @Uom,
    @Qty, @UnitPrice, @DiscountPct, @GstPct, @Tax,
    @TaxCodeId,
    @LineAmount,
    @TaxAmount,
    @Description,
    @BudgetLineId
);";

            var lineId = await Connection.ExecuteScalarAsync<int>(
                sql,
                new
                {
                    SiId = siId,
                    SourceType = sourceType,
                    SourceLineId = l.SourceLineId,
                    l.ItemId,
                    l.ItemName,
                    Uom = l.Uom,
                    l.Qty,
                    l.UnitPrice,
                    l.DiscountPct,
                    l.GstPct,
                    l.Tax,
                    l.TaxCodeId,
                    l.LineAmount,
                    TaxAmount = l.TaxAmount,
                    Description = string.IsNullOrWhiteSpace(l.Description)
                        ? l.ItemName
                        : l.Description,
                    l.BudgetLineId
                });

            await RecalculateTotalAsync(siId);
            return lineId;
        }

        public async Task UpdateLineAsync(
            int lineId,
            decimal qty,
            decimal unitPrice,
            decimal discountPct,
            decimal gstPct,
            string tax,
            int? taxCodeId,
            decimal? lineAmount,
            decimal? taxAmount,
            string? description,
            int? budgetLineId,
            int userId)
        {
            const string updateSql = @"
UPDATE dbo.SalesInvoiceLine
SET Qty         = @Qty,
    UnitPrice   = @UnitPrice,
    DiscountPct = @DiscountPct,
    GstPct      = @GstPct,
    Tax         = @Tax,
    TaxCodeId   = @TaxCodeId,
    LineAmount  = @LineAmount,
    TaxAmount   = @TaxAmount,
    Description = @Description,
    BudgetLineId= @BudgetLineId
WHERE Id = @Id;";

            await Connection.ExecuteAsync(updateSql, new
            {
                Id = lineId,
                Qty = qty,
                UnitPrice = unitPrice,
                DiscountPct = discountPct,
                GstPct = gstPct,
                Tax = tax,
                TaxCodeId = taxCodeId,
                LineAmount = lineAmount,
                TaxAmount = taxAmount,
                Description = description,
                BudgetLineId = budgetLineId
            });

            var siId = await Connection.ExecuteScalarAsync<int>(
                "SELECT SiId FROM dbo.SalesInvoiceLine WHERE Id = @Id",
                new { Id = lineId });

            await RecalculateTotalAsync(siId);
        }

        public async Task RemoveLineAsync(int lineId)
        {
            var siId = await Connection.ExecuteScalarAsync<int>(
                "SELECT SiId FROM dbo.SalesInvoiceLine WHERE Id = @Id",
                new { Id = lineId });

            await Connection.ExecuteAsync(
                @"DELETE FROM dbo.SalesInvoiceLine WHERE Id=@Id;",
                new { Id = lineId });

            await RecalculateTotalAsync(siId);
        }

        private async Task RecalculateTotalAsync(int siId)
        {
            const string sql = @"
UPDATE si
SET
    si.Subtotal  = x.Subtotal,
    si.TaxAmount = x.TaxAmount,
    si.Total     = x.LinesTotal + si.ShippingCost
FROM dbo.SalesInvoice si
CROSS APPLY (
    SELECT
        Subtotal  = ISNULL(SUM(CAST(sil.Qty * sil.UnitPrice AS DECIMAL(18,2))), 0),
        TaxAmount = ISNULL(SUM(ISNULL(sil.TaxAmount,0)), 0),
        LinesTotal = ISNULL(SUM(
            CASE 
                WHEN sil.LineAmount IS NOT NULL THEN sil.LineAmount
                ELSE
                    -- fallback calc if LineAmount missing
                    CASE 
                        WHEN ISNULL(sil.GstPct,0) = 0 
                             OR UPPER(ISNULL(sil.Tax,'EXEMPT')) = 'EXEMPT' THEN
                            sil.Qty * sil.UnitPrice * (1 - (sil.DiscountPct / 100.0))
                        WHEN UPPER(ISNULL(sil.Tax,'EXCLUSIVE')) = 'EXCLUSIVE' THEN
                            (sil.Qty * sil.UnitPrice * (1 - (sil.DiscountPct / 100.0)))
                            * (1 + (sil.GstPct / 100.0))
                        WHEN UPPER(ISNULL(sil.Tax,'INCLUSIVE')) = 'INCLUSIVE' THEN
                            sil.Qty * sil.UnitPrice * (1 - (sil.DiscountPct / 100.0))
                        ELSE
                            sil.Qty * sil.UnitPrice * (1 - (sil.DiscountPct / 100.0))
                    END
            END
        ), 0)
    FROM dbo.SalesInvoiceLine sil
    WHERE sil.SiId = @SiId
) x
WHERE si.Id = @SiId;";

            await Connection.ExecuteAsync(sql, new { SiId = siId });
        }
    }
}
