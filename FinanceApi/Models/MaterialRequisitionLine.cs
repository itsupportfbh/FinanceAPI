namespace FinanceApi.Models
{
    public class MaterialRequisitionLine
    {
        public int Id { get; set; }
        public int MaterialReqId { get; set; }
        public int ItemId { get; set; }

        public string? ItemCode { get; set; }

        public string? ItemName { get; set; }

        public int? UomId { get; set; }

        public string? UomName { get; set; }

        public decimal Qty { get; set; }

        public DateTime CreatedDate { get; set; } = DateTime.UtcNow;

        public int ReceivedQty { get; set; }
    }
}
