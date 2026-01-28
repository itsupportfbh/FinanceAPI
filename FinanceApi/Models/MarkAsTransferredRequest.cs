namespace FinanceApi.Models
{
    public class MarkAsTransferredRequest
    {
        public int ItemId { get; set; }
        public int WarehouseId { get; set; }
        public int? BinId { get; set; } // optional

        public int? SupplierId { get; set; }
    }
}
