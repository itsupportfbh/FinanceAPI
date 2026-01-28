namespace FinanceApi.ModelDTO
{
    public class DaybookDTO
    {
        public DateTime TransDate { get; set; }
        public string VoucherNo { get; set; }
        public string VoucherType { get; set; }
        public string VoucherName { get; set; }
        public string AccountHeadName { get; set; }
        public string Description { get; set; }
        public decimal Debit { get; set; }
        public decimal Credit { get; set; }
        public decimal RunningBalance { get; set; }
    }
}
