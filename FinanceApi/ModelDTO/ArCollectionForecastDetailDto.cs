public class ArCollectionForecastDetailDto
{
    public int InvoiceId { get; set; }
    public string InvoiceNo { get; set; } = string.Empty;

    public DateTime InvoiceDate { get; set; }
    public DateTime DueDate { get; set; }

    public decimal Balance { get; set; }

    /// <summary>
    /// '0-7', '8-14', '15-30', '>30'
    /// </summary>
    public string BucketName { get; set; } = string.Empty;
}