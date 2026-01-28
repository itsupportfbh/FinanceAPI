namespace FinanceApi.ModelDTO
{
    public class GeneralLedgerLineDTO
    {
        public DateTime TransDate { get; set; }
        public string SourceType { get; set; } = string.Empty;   // MJ / SI / CN
        public int SourceId { get; set; }
        public string SourceNo { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;

        public int HeadId { get; set; }
        public int HeadCode { get; set; }
        public string HeadName { get; set; } = string.Empty;

        public decimal Debit { get; set; }
        public decimal Credit { get; set; }

        public decimal Net => Debit - Credit;   
    }
}
