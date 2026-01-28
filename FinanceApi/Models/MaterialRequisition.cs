using Microsoft.AspNetCore.Http.HttpResults;

namespace FinanceApi.Models
{
    public class MaterialRequisition
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
        public int BinId { get; set; }

        // Navigation
        public ICollection<MaterialRequisitionLine> Lines { get; set; } = new List<MaterialRequisitionLine>();
    }
}
