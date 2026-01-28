namespace FinanceApi.ModelDTO
{
    public class ItemPriceDto
    {
        public long? Id { get; set; }
        public long SupplierId { get; set; }
        public decimal? Qty { get; set; }
        public decimal Price { get; set; }
        public string? Barcode { get; set; }
        public int WarehouseId { get; set; }

        public bool IsTransfered { get; set; }
    }
}
