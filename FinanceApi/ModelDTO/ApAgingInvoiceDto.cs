public class ApAgingInvoiceDto
{
    public int InvoiceId { get; set; }
    public string InvoiceNo { get; set; } = string.Empty;
    public DateTime InvoiceDate { get; set; }
    public DateTime DueDate { get; set; }
    public int AgeDays { get; set; }
    public string BucketName { get; set; } = string.Empty;

    public int SupplierId { get; set; }
    public string SupplierName { get; set; } = string.Empty;

    public decimal OriginalAmount { get; set; }
    public decimal PaidAmount { get; set; }
    public decimal CreditAmount { get; set; }
    public decimal Balance { get; set; }
}
