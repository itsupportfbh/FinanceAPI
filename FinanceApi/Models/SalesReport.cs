namespace FinanceApi.Models
{
    public class SalesReport
    {
        public string ItemName { get; set; }
        public string Uom { get; set; }
        public decimal Quantity { get; set; }
        public string SalesPerson { get; set; }
        public DateTime CreatedDate { get; set; }
        public decimal? Discount { get; set; }
        public string Tax { get; set; }
        public decimal? UnitPrice { get; set; }

        // Item Master
        public string Sku { get; set; }
        public string Category { get; set; }

        // Customer Location
        public string Location { get; set; }

        // Sales Order Summary
        public decimal? GrossSales { get; set; }
        public decimal? NetSales { get; set; }
        public decimal? GstPct { get; set; }

        // Purchase / Cost
        public decimal? PurchaseCost { get; set; }

        // Computed Value
        public decimal? TaxAmount { get; set; }
    }
}
