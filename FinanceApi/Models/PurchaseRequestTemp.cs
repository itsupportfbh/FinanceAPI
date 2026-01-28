namespace FinanceApi.Models
{
    public class PurchaseRequestTemp
    {
        public int Id { get; set; }
        public string Requester { get; set; }
        public int? DepartmentID { get; set; }
        public DateTime? DeliveryDate { get; set; }
        public bool MultiLoc { get; set; }
        public bool Oversea { get; set; }
        public string PRLines { get; set; }     // JSON
        public string Description { get; set; }
        public int Status { get; set; }         // 0 = Draft
        public bool IsActive { get; set; }
        public string CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public string UpdatedBy { get; set; }
        public DateTime UpdatedDate { get; set; }
    }
}
