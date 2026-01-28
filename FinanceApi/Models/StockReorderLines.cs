namespace FinanceApi.Models
{
    public class StockReorderLines : BaseEntity
    {
        public int Id { get; set; }
        public int StockReorderId { get; set; }
        public int WarehouseTypeId { get; set; }
        public long ItemId { get; set; }
        public decimal OnHand { get; set; }
        public decimal Min { get; set; }
        public decimal Max { get; set; }
        public decimal ReorderQty { get; set; }        
        public int LeadDays { get; set; }
        public int UsageHorizon { get; set; }
        public int Suggested { get; set; }

        public int Status { get; set; }

        public bool Selected { get; set; }

        public List<StockReorderLineSupplier> SupplierBreakdown { get; set; } = new();


    }
    public sealed class StockReorderLineSupplier
    {
        public int Id { get; set; }                 // <-- needed for upsert
        public int StockReorderLineId { get; set; } // filled server-side
        public long SupplierId { get; set; }
        public decimal? Price { get; set; }
        public decimal? Qty { get; set; }
        public bool Selected { get; set; }
        public bool IsActive { get; set; } = true;
    }
}
