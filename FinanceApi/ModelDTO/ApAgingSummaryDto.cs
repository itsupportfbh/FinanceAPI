public class ApAgingSummaryDto
{
    public int SupplierId { get; set; }
    public string SupplierName { get; set; } = string.Empty;

    public int InvoiceCount { get; set; }
    public decimal TotalOutstanding { get; set; }

    public decimal Bucket0_30 { get; set; }
    public decimal Bucket31_60 { get; set; }
    public decimal Bucket61_90 { get; set; }
    public decimal Bucket90Plus { get; set; }
}
