namespace FinanceApi.ModelDTO
{
    public class ApplyGrnLine
    {
        public string ItemCode { get; set; } = string.Empty;   // SKU
        public long? SupplierId { get; set; }

        public long WarehouseId { get; set; }
        public long? BinId { get; set; }
        public long? StrategyId { get; set; }

        // ✅ this is what the repo expects and uses
        public decimal QtyDelta { get; set; }

        public bool BatchFlag { get; set; }
        public bool SerialFlag { get; set; }

        // optional price write (ItemPrice)
        public string? Barcode { get; set; }
        public decimal? Price { get; set; }

        // optional traceability
        public string? BatchSerial { get; set; }
        public DateTime? Expiry { get; set; }

        public string? Remarks { get; set; }
    }
}
