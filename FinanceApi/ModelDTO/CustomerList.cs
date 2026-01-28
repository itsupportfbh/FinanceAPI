namespace FinanceApi.ModelDTO
{
    public class CustomerList
    {
        public int CustomerId { get; set; }
        public int KycId { get; set; }
        public string CustomerName { get; set; }
        public string ContactNumber { get; set; }
        public string PointOfContactPerson { get; set; }
        public string Email { get; set; }
        public decimal? CreditAmount { get; set; }

        public int CountryId { get; set; }
        public int LocationId { get; set; }
        public int CustomerGroupId { get; set; }
        public int? BudgetLineId { get; set; }
        
        public int PaymentTermId { get; set; }

        public string PaymentTermsName { get; set; }
        public string CustomerGroupName { get; set; }
        public string CountryName { get; set; }
        public string LocationName { get; set; }

        public bool? IsApproved { get; set; }
        public string ApprovedBy { get; set; }
        public string DLImage { get; set; }
        public string BSImage { get; set; }
        public string UtilityBillImage { get; set; }
        public string ACRAImage { get; set; }


    }
}
