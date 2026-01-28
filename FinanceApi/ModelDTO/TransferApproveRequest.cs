namespace FinanceApi.ModelDTO
{
    public class TransferApproveRequest
    {
        public int StockId { get; set; }
        public int ItemId { get; set; }

        public int WarehouseId { get; set; }      // FromWarehouseId
        public int? BinId { get; set; }

        public int ToWarehouseId { get; set; }
        public int ToBinId { get; set; }

        public int TransferQty { get; set; }
        public int RequestedQty { get; set; }     // ✅ needed for partial/full

        public int? SupplierId { get; set; }
        public string? Remarks { get; set; }

        public int? MrId { get; set; }
        public string? ReqNo { get; set; }
        public string? Sku { get; set; }

        public string? UpdatedBy { get; set; }    // optional
    }
}
