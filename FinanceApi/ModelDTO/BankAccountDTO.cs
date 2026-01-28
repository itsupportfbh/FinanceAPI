namespace FinanceApi.ModelDTO
{
    public class BankAccountDTO
    {
        public int BankId { get; set; }
        public string BankName { get; set; }
        public string HeadCode { get; set; }
        public decimal OpeningBalance { get; set; }
        public decimal AvailableBalance { get; set; }
    }
}
