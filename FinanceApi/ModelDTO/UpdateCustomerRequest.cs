namespace FinanceApi.ModelDTO
{
    public class UpdateCustomerRequest
    {
        public int CustomerId { get; set; }
        public int? KycId { get; set; }

        public string CustomerName { get; set; }
        public int CountryId { get; set; }
        public int LocationId { get; set; }
        public string ContactNumber { get; set; }
        public string PointOfContactPerson { get; set; }
        public string Email { get; set; }
        public int CustomerGroupId { get; set; }
        public int BudgetLineId { get; set; }
        public int PaymentTermId { get; set; }
        public decimal CreditAmount { get; set; }
        public bool IsApproved { get; set; }
        public int? ApprovedBy { get; set; }

        // File uploads (from form)
        public IFormFile? DrivingLicence { get; set; }
        public IFormFile? UtilityBill { get; set; }
        public IFormFile? BankStatement { get; set; }
        public IFormFile? Acra { get; set; }

        // File names stored in DB (optional)
        public string? DlImageName { get; set; }
        public string? UtilityBillImageName { get; set; }
        public string? BsImageName { get; set; }
        public string? AcraImageName { get; set; }
    }
}
