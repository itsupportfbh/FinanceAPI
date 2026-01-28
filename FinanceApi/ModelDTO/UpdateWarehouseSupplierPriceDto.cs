namespace FinanceApi.ModelDTO
{
    public class UpdateWarehouseSupplierPriceDto
    {
        public string ItemCode { get; set; } = string.Empty;   // SKU

        public long WarehouseId { get; set; }
        public decimal? Qty { get; set; }
        public long? BinId { get; set; }
        public long? StrategyId { get; set; }
        public decimal QtyDelta { get; set; }

        public bool BatchFlag { get; set; }
        public bool SerialFlag { get; set; }

        public long? SupplierId { get; set; }
        public decimal? Price { get; set; }
        public string? Barcode { get; set; }

        public string? Remarks { get; set; }
        public string? UpdatedBy { get; set; }
    }
}
