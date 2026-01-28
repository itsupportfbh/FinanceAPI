using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    [Table("ItemMaster")]
    public class ItemMaster
    {
        public long Id { get; set; }
        public string Sku { get; set; } = "";
        public string Name { get; set; } = "";
        public string? Category { get; set; }
        public string? Uom { get; set; }
       
        public long? CostingMethodId { get; set; }
        public long? TaxCodeId { get; set; }
        public string? Specs { get; set; }
        public string? PictureUrl { get; set; }
        public bool IsActive { get; set; } = true;

        // convenience from your left join
        public decimal OnHand { get; set; }
        public decimal Reserved { get; set; }
        public decimal Available { get; set; }
        public string? CreatedBy { get; set; }
        public DateTime? CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public DateTime? ExpiryDate { get; set; }
    }
}
