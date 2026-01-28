namespace FinanceApi.ModelDTO
{
    public class CreateCustomerWithKycRequest
    {
        public string? CustomerName { get; set; }
        public int CountryId { get; set; }
        public int LocationId { get; set; }
        public long? ContactNumber { get; set; }
        public string? PointOfContactPerson { get; set; }
        public string? Email { get; set; }
        public int? CustomerGroupId { get; set; }

        public int? BudgetLineId { get; set; }
        public int? PaymentTermId { get; set; }
        public decimal? CreditAmount { get; set; }
        public int? CreatedBy { get; set; }

        // files
        public IFormFile? DrivingLicence { get; set; }
        public IFormFile? UtilityBill { get; set; }
        public IFormFile? BankStatement { get; set; }
        public IFormFile? Acra { get; set; }

        public int? ApprovedBy { get; set; }
        public bool? IsApproved { get; set; }
    }
}
