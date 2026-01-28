namespace FinanceApi.Models
{
    public class Driver
    {
        public int Id { get; set; }
        public string DriverName { get; set; }
        public long MobileNumber { get; set; }
        public string LicenseNumber { get; set; }
        public DateTime LicenseExpiryDate { get; set; }
        public string? NricOrId { get; set; }
        public bool IsActive { get; set; } = true;
        public DateTime? CreatedDate { get; set; } = DateTime.UtcNow;
        public int? CreatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public int? UpdatedBy { get; set; }
    }
}
