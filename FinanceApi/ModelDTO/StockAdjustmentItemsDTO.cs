namespace FinanceApi.ModelDTO
{
    public class StockAdjustmentItemsDTO
    {
        public int ItemId { get; set; }
        public string Name { get; set; }
        public string Sku { get; set; }

        public int Available { get; set; }

        public int BinId { get; set; }
        public int WarehouseId { get; set; }
        public int SupplierId { get; set; }

        public string BinName { get; set; }
        public string WarehouseName { get; set; }

        public string SupplierName { get; set; }
        public int Qty { get; set; }
        public int Price { get; set; }
    }
}
