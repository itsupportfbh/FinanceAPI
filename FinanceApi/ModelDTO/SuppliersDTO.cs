namespace FinanceApi.ModelDTO
{
    public class SuppliersDTO
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public string Code { get; set; }

        public int StatusId { get; set; }

        public int? LeadTime { get; set; }

        public int CountryId { get; set; }

        public int? TermsId { get; set; }

        public int CurrencyId { get; set; }

        public string TaxReg { get; set; }

        public int? IncotermsId { get; set; }

        public string Contact { get; set; }

        public string Email { get; set; }

        public string Phone { get; set; }

        public string Address { get; set; }

        public string BankName { get; set; }

        public string BankAcc { get; set; }

        public string BankSwift { get; set; }

        public string BankBranch { get; set; }

        public DateTime CreatedDate { get; set; }

        public DateTime? UpdatedDate { get; set; }

        public string CreatedBy { get; set; }

        public string UpdatedBy { get; set; }
        public bool? IsActive { get; set; }
        public string ItemID { get; set; }

        public int BudgetLineId { get; set; }
        public string ComplianceDocuments { get; set; }
    }
}
