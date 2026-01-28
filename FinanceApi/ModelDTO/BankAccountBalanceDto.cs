namespace FinanceApi.ModelDTO
{
    public class BankAccountBalanceDto
    {
        public int Id { get; set; }              // HeadId
        public string HeadCode { get; set; } = string.Empty;
        public string HeadName { get; set; } = string.Empty;
        public decimal OpeningBalance { get; set; }
        public decimal AvailableBalance { get; set; }
    }
}
