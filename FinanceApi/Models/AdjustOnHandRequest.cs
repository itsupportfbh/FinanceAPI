namespace FinanceApi.Models
{
    public class AdjustOnHandRequest
    {
        public long ItemId { get; set; }
        public long WarehouseId { get; set; }
        public long? BinId { get; set; }
        public long? SupplierId { get; set; }

        // 🔹 Qty to subtract from ItemWarehouseStock
        public decimal FaultQty { get; set; }

        // 🔹 Full new OnHand value to update in ItemPrice
        public decimal FinalOnHand { get; set; }

        public long? StockIssueId { get; set; }
        public long? ApprovedBy { get; set; }   // approver ID
        public string? UpdatedBy { get; set; }
    }

    public sealed class AdjustOnHandResult
    {
        public decimal OnHand { get; set; }
        public decimal Reserved { get; set; }
        public decimal Available { get; set; }
        public decimal? PriceQty { get; set; }      // optional echo of ItemPrice.Qty after update
    }
}