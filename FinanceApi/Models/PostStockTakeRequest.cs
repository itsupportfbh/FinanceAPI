namespace FinanceApi.Models
{
    public class PostStockTakeRequest
    {
        public string? Reason { get; set; }          // optional global reason (fallback)
        public string? Remarks { get; set; }        // optional global remarks (fallback)
        public bool ApplyToStock { get; set; } = true;   // true: update ItemWarehouseStock
        public bool MarkPosted { get; set; } = true;     // true: move header to Posted
        public DateTime? TxnDate { get; set; }           // optional transaction date override
        public bool OnlySelected { get; set; } = true;
    }
}
