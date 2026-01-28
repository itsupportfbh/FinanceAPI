namespace FinanceApi.Models
{
    public class StockListViewInfo
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string Sku { get; set; }

        public int WarehouseId { get; set; }
        public string WarehouseName { get; set; }

        public int BinId { get; set; }
        public string BinName { get; set; }

        public int OnHand {  get; set; }

        public int MinQty { get; set; }
        public int MaxQty { get; set; }

        public int Reserved { get; set; }

        public DateTime ExpiryDate { get; set; }
        public string Category { get; set; }
        public string Uom {  get; set; }
        public int SupplierId { get; set; }
        public int Available {  get; set; }
        public string SupplierName { get; set; }
        public int Qty {  get; set; }
    }
}
