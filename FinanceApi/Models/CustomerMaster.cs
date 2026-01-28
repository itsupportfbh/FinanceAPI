namespace FinanceApi.Models
{
    public class CustomerMaster
    {
        public int Id { get; set; }

        public string CustomerName { get; set; }

        public int CountryId { get; set; }

        public int LocationId { get; set; }

        public long? ContactNumber { get; set; }

        public string PointOfContactPerson { get; set; }

        public string Email { get; set; }

        public int? CustomerGroupId { get; set; }
        public int? BudgetLineId { get; set; }

        public int? PaymentTermId { get; set; }

        public decimal? CreditAmount { get; set; }

        public int? KycId { get; set; }

        public DateTime CreatedDate { get; set; }

        public int? CreatedBy { get; set; }

        public DateTime? UpdatedDate { get; set; }

        public int? UpdatedBy { get; set; }

        public bool IsActive { get; set; }
    }
}
