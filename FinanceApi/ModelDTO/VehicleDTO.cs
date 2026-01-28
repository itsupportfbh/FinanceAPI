namespace FinanceApi.ModelDTO
{
    public class VehicleDTO
    {
        public int Id { get; set; }
        public string VehicleNo { get; set; } = string.Empty;
        public string? VehicleType { get; set; }
        public decimal? Capacity { get; set; }
        public string? CapacityUom { get; set; }
        public bool IsActive { get; set; }
    }
}
