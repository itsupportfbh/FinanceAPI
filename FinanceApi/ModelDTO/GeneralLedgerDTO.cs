namespace FinanceApi.ModelDTO
{
    public class GeneralLedgerDTO
    {
        public int HeadId { get; set; }
        public int HeadCode { get; set; }
        public string HeadName { get; set; } = string.Empty;
        public int ParentHead { get; set; }

        public string HeadType { get; set; } = string.Empty;      // 'A', 'L', ...
        public string RootHeadType { get; set; } = string.Empty;  

        public decimal OpeningBalance { get; set; }
        public decimal Debit { get; set; }
        public decimal Credit { get; set; }
        public decimal Balance { get; set; }

        public bool IsControl { get; set; }   // true for AR / AP control heads
    }
}
