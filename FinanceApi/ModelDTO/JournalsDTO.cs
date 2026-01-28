namespace FinanceApi.ModelDTO
{
    public class JournalsDTO
    {
        public int Id { get; set; }
        public string? JournalNo { get; set; }
        public DateTime JournalDate { get; set; }

        public decimal DebitAmount { get; set; }
        public decimal CreditAmount { get; set; }

        public bool IsRecurring { get; set; }
        public string? RecurringFrequency { get; set; }
        public bool IsPosted { get; set; }

        public string Description { get; set; } = string.Empty;
    }
}
