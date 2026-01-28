namespace FinanceApi.Models
{
    public class StockReorderWarehouseItems
    {
        public long ItemId { get; set; }
        public string ItemName { get; set; } = string.Empty;
        public string? Sku { get; set; }

        // Warehouse / Bin
        public long WarehouseId { get; set; }
        public string WarehouseName { get; set; } = string.Empty;
        public long? BinId { get; set; }
        public string? BinName { get; set; }

        // Inventory levels
        public decimal OnHand { get; set; }
        public decimal Reserved { get; set; }
        public decimal MinQty { get; set; }
        public decimal? ReorderQty { get; set; }   // can be null if you only use Max
        public decimal? MaxQty { get; set; }   // can be null if you only use Reorder

        // Planning helpers (kept for UI calc even if set to 0 in this query)
        public int LeadDays { get; set; }           // from supplier; 0 in simplified query
        public decimal UsageHorizon { get; set; }           // 0 in simplified query
        public decimal SafetyStock { get; set; }

        public List<StockReorderSupplier> Suppliers { get; set; } = new();
    }

    public sealed class StockReorderSupplier
    {
        public long SupplierId { get; set; }
        public string SupplierName { get; set; } = "";
        public decimal Price { get; set; }
        public decimal Qty { get; set; }
    }
}
