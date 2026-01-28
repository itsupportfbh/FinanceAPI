using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    public enum AccountType
    {
        Debit = 1,
        Credit = 2
    }
    public class OpeningBalance : BaseEntity
    {
        public int Id { get; set; }
       
        public int BudgetLineId { get; set; }
        public decimal OpeningBalanceAmount { get; set; }

        public bool IsActive { get; set; }
    }

    public class OpeningBalanceDto

    {
        public int Id { get; set; }

        public int BudgetLineId { get; set; }
        public decimal OpeningBalanceAmount { get; set; }
        public bool IsActive { get; set; }
    }
}
