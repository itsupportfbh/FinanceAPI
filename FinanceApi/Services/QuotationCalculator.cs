// Services/QuotationCalculator.cs
using FinanceApi.ModelDTO;
using System.Globalization;
using System.Text.RegularExpressions;

public static class QuotationCalculator
{
    public static void Compute(QuotationDTO q, Func<int, int, decimal>? getTaxRate = null)
    {
        if (q == null) throw new ArgumentNullException(nameof(q));

        decimal subtotal = 0m;
        decimal taxTotal = 0m;
        bool needsHod = false;

        var lines = q.Lines ?? Enumerable.Empty<QuotationLineDTO>();

        foreach (var l in lines)
        {
            // Coalesce if nullable; if your props are non-nullable, these lines are still fine.
            var qty = l.Qty is decimal dq ? dq : 0m;
            var price = l.UnitPrice is decimal up ? up : 0m;
            var disc = l.DiscountPct is decimal dp ? dp : 0m;

            // Clamp discount 0..100
            disc = Math.Clamp(disc, 0m, 100m);
            if (disc > 10m) needsHod = true;

            // Net before tax
            var net = qty * price * (1 - disc / 100m);

            // Tax rate resolution (prefer delegate, else parse label like "GST 8%")
            decimal rate = 0m;
            if (getTaxRate != null && l.TaxCodeId.HasValue)
            {
                rate = getTaxRate(l.TaxCodeId.Value, l.ItemId);
            }
            else if (!string.IsNullOrWhiteSpace(l.TaxCodeLabel) && TryParsePercent(l.TaxCodeLabel!, out var r))
            {
                rate = r;
            }

            // Round
            var netRounded = Math.Round(net, 2, MidpointRounding.AwayFromZero);
            var taxRounded = Math.Round(net * (rate / 100m), 2, MidpointRounding.AwayFromZero);
            var total = netRounded + taxRounded;

            // Assign (works for decimal and decimal?)
            l.LineNet = netRounded;
            l.LineTax = taxRounded;
            l.LineTotal = total;

            // Accumulate (handle header being nullable too)
            subtotal += netRounded;
            taxTotal += taxRounded;
        }

        // Header totals (coalesce if nullable)
        q.Subtotal = subtotal;
        q.TaxAmount = taxTotal;
        q.Rounding = q.Rounding is decimal r0 ? r0 : 0m;
        q.NeedsHodApproval = needsHod;
        q.GrandTotal = q.Subtotal + q.TaxAmount + q.Rounding;
    }

    // Pulls the last number in a string like "GST 8%" or "VAT 7.5"
    private static bool TryParsePercent(string s, out decimal value)
    {
        value = 0m;
        if (string.IsNullOrWhiteSpace(s)) return false;

        var m = Regex.Matches(s, @"[-+]?\d+(\.\d+)?");
        if (m.Count == 0) return false;

        return decimal.TryParse(
            m[^1].Value,
            NumberStyles.Number,
            CultureInfo.InvariantCulture,
            out value
        );
    }
}
