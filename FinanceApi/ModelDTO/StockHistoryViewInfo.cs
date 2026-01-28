namespace FinanceApi.ModelDTO
{
    public class StockHistoryViewInfo
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Sku { get; set; }
        public int FromWarehouseID { get; set; }
        public int ToWarehouseID { get; set; }
        public string FromWarehouseName { get; set; }

        public string ToWarehouseName { get; set; }

        public decimal TransferQty { get; set; }

        public int Available {  get; set; }
    }
}
