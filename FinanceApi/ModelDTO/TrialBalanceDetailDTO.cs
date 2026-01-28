public class TrialBalanceDetailDTO
{
    public DateTime TransDate { get; set; }
    public string SourceType { get; set; }
    public string SourceNo { get; set; }
    public string Description { get; set; }
    public decimal Debit { get; set; }
    public decimal Credit { get; set; }
}