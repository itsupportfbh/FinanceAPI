namespace FinanceApi.ModelDTO
{
    public class PurchaseRequestTempDto
    {
        public int Id { get; set; }                 // 0 for create
        public string Requester { get; set; }
        public int? DepartmentID { get; set; }
        public DateTime? DeliveryDate { get; set; }
        public bool MultiLoc { get; set; }
        public bool Oversea { get; set; }
        public string PRLines { get; set; }         // JSON string
        public string Description { get; set; }
        public int Status { get; set; } = 0;        // default Draft
        public bool IsActive { get; set; } = true;
        public string UserId { get; set; } = default!;
        public string DepartmentName {  get; set; }
    }
}
