// ModelDTO/TB/TrialBalanceDTO.cs
namespace FinanceApi.ModelDTO.TB
{
    public class TrialBalanceDTO
    {
        public int HeadId { get; set; }
        public string HeadCode { get; set; } = string.Empty;
        public string HeadName { get; set; } = string.Empty;
        public int? ParentHead { get; set; }

        public decimal OpeningDebit { get; set; }
        public decimal OpeningCredit { get; set; }

        public decimal ClosingDebit { get; set; }
        public decimal ClosingCredit { get; set; }
    }
}
