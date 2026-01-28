namespace FinanceApi.Models
{
    public class KYC
    {
        public int Id { get; set; }

        public string DLImage { get; set; }
        public string UtilityBillImage { get; set; }
        public string BSImage { get; set; }
        public string ACRAImage { get; set; }

        public string DLImageName { get; set; }
        public string UtilityBillImageName { get; set; }
        public string BSImageName { get; set; }
        public string ACRAImageName { get; set; }

        public int? ApprovedBy { get; set; }
        public bool? IsApproved { get; set; }
        public bool? IsActive { get; set; }
        public DateTime CreatedDate { get; set; }
        public int? CreatedBy { get; set; }

        public DateTime? UpdatedDate { get; set; }
        public int? UpdatedBy { get; set; }
    }
}
