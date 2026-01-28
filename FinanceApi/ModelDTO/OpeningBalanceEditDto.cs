// FinanceApi/ModelDTO/OpeningBalanceEditDto.cs
namespace FinanceApi.ModelDTO
{
    public class OpeningBalanceEditDto
    {
        public int HeadId { get; set; }          // ChartOfAccount.Id (BudgetLineId)
        public decimal OpeningDebit { get; set; }
        public decimal OpeningCredit { get; set; }
        public DateTime? AsOfDate { get; set; }  // optional – use null for now
        public int? CompanyId { get; set; }      // optional, if you add later
        public string? UserName { get; set; }
    }
}
