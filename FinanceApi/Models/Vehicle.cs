namespace FinanceApi.Models
{
    public class Vehicle
    {
        public int Id { get; set; }
        public string VehicleNo { get; set; } = string.Empty;     // NVARCHAR(20)
        public string? VehicleType { get; set; }                  // NVARCHAR(20)
        public decimal? Capacity { get; set; }                    // DECIMAL(10,2)
        public string? CapacityUom { get; set; }                  // 'KG','CBM'
        public bool IsActive { get; set; } = true;

        public int? CreatedBy { get; set; }
        public DateTime CreatedOn { get; set; }                   // DEFAULT SYSUTCDATETIME()
        public int? UpdatedBy { get; set; }
        public DateTime? UpdatedOn { get; set; }
    }
}
