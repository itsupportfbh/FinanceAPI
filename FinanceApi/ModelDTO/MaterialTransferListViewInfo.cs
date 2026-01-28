namespace FinanceApi.ModelDTO
{
    public class MaterialTransferListViewInfo
    {
        public int StockId {  get; set; }
        public int ItemId { get; set; }
        public string ItemName {  get; set; }

        public string Sku {  get; set; }
        public int FromWarehouseId { get; set; }
        public int ToWarehouseId { get; set;}

        public string FromWarehouseName { get; set; }
        public string ToWarehouseName { get;set; }

        public int BinId { get; set; }
        public int ToBinId { get; set; }

        public string BinName { get; set; }
        public string ToBinName { get; set; }

        public int OnHand {  get; set; }

        public int Available {  get; set; }

        public int MrId { get; set; }

        public string ReqNo { get; set; }

        public int SupplierId { get; set; }
        public string SupplierName { get; set; }

        public int RequestQty { get; set; }
        public int status { get; set; }

        public int TransferQty {  get; set; }

    }
}
