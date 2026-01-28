namespace FinanceApi.ModelDTO
{
    public class ArAdvanceDto
    {
        public int CustomerId { get; set; }
        public int? SalesOrderId { get; set; }   // 🔹 optional
        public DateTime AdvanceDate { get; set; }
        public decimal Amount { get; set; }
        public int? BankAccountId { get; set; }
        public string? PaymentMode { get; set; }
        public string? Remarks { get; set; }
    }

    public class ArOpenAdvanceDto
    {
        public int Id { get; set; }
        public string AdvanceNo { get; set; }
        public DateTime AdvanceDate { get; set; }
        public decimal Amount { get; set; }
        public decimal BalanceAmount { get; set; }
        public int? SalesOrderId { get; set; }
        public string? OrderNo { get; set; }
    }
}
