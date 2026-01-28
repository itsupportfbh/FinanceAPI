// Repositories/QuotationRepository.cs
using System.Data;
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;

namespace FinanceApi.Repositories
{
    public class QuotationRepository : DynamicRepository, IQuotationRepository
    {
        public QuotationRepository(IDbConnectionFactory cf) : base(cf) { }

        public async Task<IEnumerable<QuotationListDTO>> GetAllAsync()
        {
            const string headerSql = @"
SELECT q.Id,
       q.Number,
       q.Status,
       q.CustomerId,
       c.CustomerName AS CustomerName,
       q.CurrencyId,
       q.FxRate,
       q.PaymentTermsId,
       q.DeliveryDate,                 -- existing
       q.Remarks,                      -- ✅ NEW (Header)
       q.DeliveryTo,                   -- ✅ NEW (Header)
       q.Subtotal,
       q.TaxAmount,
       q.Rounding,
       q.GrandTotal,
       q.NeedsHodApproval,
       pt.PaymentTermsName,
	   us.Username as CreatedUserName,
	   u.Username as ModifiedUserName
FROM dbo.Quotation q
LEFT JOIN dbo.Customer c ON c.Id = q.CustomerId
LEFT JOIN dbo.PaymentTerms pt ON pt.Id = q.PaymentTermsId
Left join dbo.[User] us on us.Id = q.CreatedBy
Left join dbo.[User] u on u.Id = q.UpdatedBy
WHERE q.IsActive = 1
ORDER BY q.Id DESC;";

            return await Connection.QueryAsync<QuotationListDTO>(headerSql);
        }

        public async Task<QuotationListDTO?> GetByIdAsync(int id)
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
       q.DeliveryDate,                 -- existing
       q.Remarks,                      -- ✅ NEW (Header)
       q.DeliveryTo,                   -- ✅ NEW (Header)
       q.Subtotal,
       q.TaxAmount,
       q.Rounding,
       q.GrandTotal,
       q.NeedsHodApproval,
       cu.CurrencyName,
       pt.PaymentTermsName
FROM dbo.Quotation q
LEFT JOIN dbo.Customer c ON c.Id = q.CustomerId
LEFT JOIN dbo.Currency cu ON cu.Id = q.CurrencyId
LEFT JOIN dbo.PaymentTerms pt ON pt.Id = q.PaymentTermsId
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
       l.TaxCodeId,
       l.TaxMode,
       l.LineNet,
       l.LineTax,
       l.LineTotal,
       l.Description ,
 i.ItemCode-- existing (you already added)
FROM dbo.QuotationLine l
LEFT JOIN dbo.Item i ON i.Id = l.ItemId
LEFT JOIN dbo.Uom  u ON u.Id = l.UomId
WHERE l.QuotationId = @Id
ORDER BY l.Id;";

            using var multi = await Connection.QueryMultipleAsync(sql, new { Id = id });
            var head = await multi.ReadFirstOrDefaultAsync<QuotationListDTO>();
            if (head is null) return null;

            head.Lines = (await multi.ReadAsync<QuotationLineDTO>()).ToList();
            return head;
        }

        public async Task<int> CreateAsync(QuotationDTO dto, int userId)
        {
            // ---------- 1) Get last quotation no ----------
            const string getLastQtNumberSql = @"
SELECT TOP (1) [Number]
FROM dbo.Quotation WITH (UPDLOCK, HOLDLOCK)
WHERE [Number] LIKE 'QT-%'
ORDER BY Id DESC;";

            var lastQt = await Connection.QueryFirstOrDefaultAsync<string>(getLastQtNumberSql);

            // ---------- 2) Compute next number ----------
            int next = 1;
            if (!string.IsNullOrWhiteSpace(lastQt) && lastQt.StartsWith("QT-"))
            {
                var numericPart = lastQt.Substring(3); // remove "QT-"
                if (int.TryParse(numericPart, out var lastNum))
                    next = lastNum + 1;
            }

            dto.Number = $"QT-{next:D4}";

            // ---------- 3) Insert header ----------
            const string insertHead = @"
INSERT INTO dbo.Quotation
(Number, Status, CustomerId, CurrencyId, FxRate, PaymentTermsId, DeliveryDate,
 Remarks, DeliveryTo,                         -- ✅ NEW (Header)
 Subtotal, TaxAmount, Rounding, GrandTotal, NeedsHodApproval,
 CreatedBy, CreatedDate, IsActive)
OUTPUT INSERTED.Id
VALUES
(@Number, @Status, @CustomerId, @CurrencyId, @FxRate, @PaymentTermsId, @DeliveryDate,
 @Remarks, @DeliveryTo,                        -- ✅ NEW (Header)
 @Subtotal, @TaxAmount, @Rounding, @GrandTotal, @NeedsHodApproval,
 @UserId, GETDATE(), 1);";

            var quotationId = await Connection.QueryFirstAsync<int>(insertHead, new
            {
                dto.Number,
                Status = (byte)dto.Status,
                dto.CustomerId,
                dto.CurrencyId,
                dto.FxRate,
                dto.PaymentTermsId,
                dto.DeliveryDate,

                // ✅ NEW (Header)
                dto.Remarks,
                dto.DeliveryTo,

                dto.Subtotal,
                dto.TaxAmount,
                dto.Rounding,
                dto.GrandTotal,
                dto.NeedsHodApproval,
                UserId = userId
            });

            // ---------- 4) Insert lines ----------
            const string insertLine = @"
INSERT INTO dbo.QuotationLine
(QuotationId, ItemId, UomId, Qty, UnitPrice, DiscountPct, TaxMode, LineNet, LineTax, LineTotal, CreatedBy, TaxCodeId, Description)
VALUES
(@QuotationId, @ItemId, @UomId, @Qty, @UnitPrice, @DiscountPct, @TaxMode, @LineNet, @LineTax, @LineTotal, @UserId, @TaxCodeId, @Description);";

            foreach (var l in dto.Lines)
            {
                await Connection.ExecuteAsync(insertLine, new
                {
                    QuotationId = quotationId,
                    l.ItemId,
                    l.UomId,
                    l.Qty,
                    l.UnitPrice,
                    l.DiscountPct,
                    l.TaxMode,
                    l.LineNet,
                    l.LineTax,
                    l.LineTotal,
                    UserId = userId,
                    l.TaxCodeId,
                    l.Description
                });
            }

            return quotationId;
        }

        public async Task UpdateAsync(QuotationDTO dto, int userId)
        {
            const string upd = @"
UPDATE dbo.Quotation
SET Number=@Number,
    Status=@Status,
    CustomerId=@CustomerId,
    CurrencyId=@CurrencyId,
    FxRate=@FxRate,
    PaymentTermsId=@PaymentTermsId,
    DeliveryDate=@DeliveryDate,
    Remarks=@Remarks,                 -- ✅ NEW (Header)
    DeliveryTo=@DeliveryTo,           -- ✅ NEW (Header)
    Subtotal=@Subtotal,
    TaxAmount=@TaxAmount,
    Rounding=@Rounding,
    GrandTotal=@GrandTotal,
    NeedsHodApproval=@NeedsHodApproval,
    UpdatedBy=@UserId,
    UpdatedDate=GETDATE()
WHERE Id=@Id;";

            await Connection.ExecuteAsync(upd, new
            {
                dto.Number,
                Status = (byte)dto.Status,
                dto.CustomerId,
                dto.CurrencyId,
                dto.FxRate,
                dto.PaymentTermsId,
                dto.DeliveryDate,

                // ✅ NEW (Header)
                dto.Remarks,
                dto.DeliveryTo,

                dto.Subtotal,
                dto.TaxAmount,
                dto.Rounding,
                dto.GrandTotal,
                dto.NeedsHodApproval,
                UserId = userId,
                dto.Id
            });

            // delete old lines
            await Connection.ExecuteAsync(
                "DELETE FROM dbo.QuotationLine WHERE QuotationId=@Id",
                new { dto.Id });

            // re-insert lines
            const string insertLine = @"
INSERT INTO dbo.QuotationLine
(QuotationId, ItemId, UomId, Qty, UnitPrice, DiscountPct, TaxMode, LineNet, LineTax, LineTotal, CreatedBy, TaxCodeId, Description)
VALUES
(@QuotationId, @ItemId, @UomId, @Qty, @UnitPrice, @DiscountPct, @TaxMode, @LineNet, @LineTax, @LineTotal, @UserId, @TaxCodeId, @Description);";

            foreach (var l in dto.Lines)
            {
                await Connection.ExecuteAsync(insertLine, new
                {
                    QuotationId = dto.Id,
                    l.ItemId,
                    l.UomId,
                    l.Qty,
                    l.UnitPrice,
                    l.DiscountPct,
                    l.TaxMode,
                    l.LineNet,
                    l.LineTax,
                    l.LineTotal,
                    UserId = userId,
                    l.TaxCodeId,
                    l.Description
                });
            }
        }

        public async Task DeactivateAsync(int id, int userId)
        {
            const string sql = @"
UPDATE dbo.Quotation
SET IsActive=0, UpdatedBy=@UserId, UpdatedDate=GETDATE()
WHERE Id=@Id;";
            await Connection.ExecuteAsync(sql, new { Id = id, UserId = userId });
        }

        // ---------- helpers (kept) ----------
        private static decimal R2(decimal v) => Math.Round(v, 2, MidpointRounding.AwayFromZero);

        private static void ComputeLine(QuotationLineDTO l, decimal headerTaxPct,
                                        out decimal lineNet, out decimal lineTax, out decimal lineTotal)
        {
            var qty = l.Qty;
            var price = l.UnitPrice;
            var disc = Math.Clamp(l.DiscountPct, 0m, 100m);
            var baseAmt = qty * price * (1 - disc / 100m);

            var mode = (l.TaxMode ?? "EXCLUSIVE").ToUpperInvariant();
            var rate = mode == "EXEMPT" ? 0m : (headerTaxPct / 100m);

            if (mode == "INCLUSIVE" && rate > 0m)
            {
                var divisor = 1m + rate;
                var net = baseAmt / divisor;
                var tax = baseAmt - net;
                lineNet = R2(net); lineTax = R2(tax); lineTotal = R2(baseAmt);
            }
            else
            {
                var net = baseAmt;
                var tax = rate > 0m ? baseAmt * rate : 0m;
                lineNet = R2(net); lineTax = R2(tax); lineTotal = R2(net + tax);
            }
        }

        private static void ComputeHeaderTotals(QuotationDTO dto)
        {
            decimal sub = 0, tax = 0;
            foreach (var l in dto.Lines)
            {
                ComputeLine(l, dto.TaxAmount, out var ln, out var lt, out var ltTot);
                sub += ln; tax += lt;
            }
            dto.Subtotal = R2(sub);
            dto.TaxAmount = R2(tax);
            dto.GrandTotal = R2(dto.Subtotal + dto.TaxAmount + dto.Rounding);
        }
    }
}
