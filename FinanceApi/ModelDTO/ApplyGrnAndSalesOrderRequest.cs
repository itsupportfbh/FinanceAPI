namespace FinanceApi.ModelDTO
{
    public class ApplyGrnAndSalesOrderRequest
    {
        public string ItemCode { get; set; } = "";
        public int? WarehouseId { get; set; }
        public int? SupplierId { get; set; }
        public int? BinId { get; set; }
        public decimal ReceivedQty { get; set; }
    }
}
