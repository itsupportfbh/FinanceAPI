namespace FinanceApi.ModelDTO
{
    public class SalesMarginReportViewInfo
    {
        public string CustomerName { get; set; }
        public string Location { get; set; }

        // Item Info
        public string Category { get; set; }
        public string ItemName { get; set; }

        // Sales Info
        public decimal NetSales { get; set; }
        public decimal GstPct { get; set; }
        public string SalesPerson { get; set; }

        // Purchase / Cost Info
        public decimal? PurchaseCost { get; set; }

        // Computed / Derived Fields
        public decimal? TaxAmount { get; set; }
        public decimal? MarginAmount { get; set; }
        public decimal? MarginPct { get; set; }

        // Invoice Info
        public string SalesInvoiceNo { get; set; }
        public DateTime? SalesInvoiceDate { get; set; }
    }
}
