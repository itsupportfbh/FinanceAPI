using FinanceApi.Models;

namespace FinanceApi.ModelDTO
{
    public class MaterialRequisitionDTO
    {
        public int Id { get; set; }
        public string ReqNo { get; set; } = string.Empty;
        public int OutletId { get; set; }
        public string RequesterName { get; set; } = string.Empty;
        public DateTime ReqDate { get; set; }
        public int Status { get; set; }
        public string? CreatedBy { get; set; }

        public DateTime CreatedDate { get; set; } = DateTime.UtcNow;
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; } = true;

        public string OutletName {  get; set; } = string.Empty;

        public int BinId { get; set; }
        public string? BinName { get; set; }
        // Navigation
        public ICollection<MaterialRequisitionLineDTO> Lines { get; set; } = new List<MaterialRequisitionLineDTO>();
    }
}
