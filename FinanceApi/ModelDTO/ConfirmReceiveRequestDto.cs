namespace FinanceApi.ModelDTO
{
    public class ConfirmReceiveRequestDto
    {
        public int StockId { get; set; }
        public int ItemId { get; set; }

        public int FromWarehouseId { get; set; }
        public int ToWarehouseId { get; set; }
        public int ToBinId { get; set; }

        public int SupplierId { get; set; }

        public int MrId { get; set; }        // ✅ Stock table match
        public string ReqNo { get; set; }    // ✅ MaterialRequisition update

        public int ReceivedQty { get; set; }
        public string TransferNo { get; set; }

        public string Remarks { get; set; }
        public int UserId { get; set; } = 1;
    }
}
