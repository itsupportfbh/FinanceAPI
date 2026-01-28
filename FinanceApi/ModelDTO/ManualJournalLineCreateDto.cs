namespace FinanceApi.ModelDTO
{
    public class ManualJournalLineCreateDto
    {
        public int AccountId { get; set; }              // ChartOfAccount.Id
        public string? Description { get; set; }        // grid line description
        public decimal Debit { get; set; }
        public decimal Credit { get; set; }
    }
}
