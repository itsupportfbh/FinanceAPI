namespace FinanceApi.Models
{
    // Models/QuotationLine.cs (request payload)
    public class QuotationLine
    {
        public int ItemId { get; set; }
        public int UomId { get; set; }                 // changed
        public decimal Qty { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal DiscountPct { get; set; }
        public int? TaxCodeId { get; set; }
        public decimal? LineNet { get; set; }
        public decimal? LineTax { get; set; }
        public decimal? LineTotal { get; set; }
        public string? Remarks { get; set; }
    }

}
