namespace FinanceApi.Models
{
    public class PaymentTerms
    {
        public int Id { get; set; }

        public string PaymentTermsName { get; set; }

        public string Description { get; set; }

        public string CreatedBy { get; set; }

        public DateTime? CreatedDate { get; set; }

        public string UpdatedBy { get; set; }

        public DateTime? UpdatedDate { get; set; }

        public bool? IsActive { get; set; }

    }
}
