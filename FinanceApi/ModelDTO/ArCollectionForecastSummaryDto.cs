public class ArCollectionForecastSummaryDto
{
    public int CustomerId { get; set; }
    public string CustomerName { get; set; } = string.Empty;

    public decimal Bucket0_7 { get; set; }
    public decimal Bucket8_14 { get; set; }
    public decimal Bucket15_30 { get; set; }
    public decimal Bucket30Plus { get; set; }

    public decimal TotalOutstanding { get; set; }
}