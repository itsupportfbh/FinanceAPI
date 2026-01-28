namespace FinanceApi.ModelDTO
{
    public class BalanceSheetViewInfo
    {
        public string Side { get; set; }              // 'Liabilities' / 'Assets'
        public int? GroupHeadId { get; set; }         // FirstChildId
        public string GroupHeadName { get; set; }     // FirstChildName

        public int HeadId { get; set; }
        public string HeadCode { get; set; }
        public string HeadName { get; set; }
        public int ParentHead { get; set; }
        public string RootHeadType { get; set; }
        public decimal? OpeningBalance { get; set; }  // null for non AR/AP
        public decimal Balance { get; set; }
    }
}
