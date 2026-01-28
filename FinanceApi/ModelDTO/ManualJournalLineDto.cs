namespace FinanceApi.ModelDTO
{
    public class ManualJournalLineDto
    {
        public int Id { get; set; }
        public int JournalId { get; set; }
        public int AccountId { get; set; }
        public string AccountName { get; set; } = string.Empty;
        public string? LineDescription { get; set; }
        public decimal Debit { get; set; }
        public decimal Credit { get; set; }
    }
}
