using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    public class Item : BaseEntity
    {
        public int Id { get; set; }
        public string ItemCode { get; set; }
        public string ItemName { get; set; }
        public int CategoryId {  get; set; }
        public int UomId { get; set; }

        [ForeignKey("UomId")]
        public Uom? Uom { get; set; }
        public int BudgetLineId { get; set; }

        [ForeignKey("BudgetLineId")]
        public ChartOfAccount? ChartOfAccount { get; set; }
    }
    public class ItemDto
    {
        public int Id { get; set; }
        public string ItemCode { get; set; }
        public string ItemName { get; set; }

        // ✅ ItemType HERE
        public string? ItemTypeId { get; set; }   // SALES / PURCHASE / BOTH

        public int UomId { get; set; }
        public string UomName { get; set; }

        public int BudgetLineId { get; set; }
        public string BudgetLineName { get; set; }

        public int CategoryId { get; set; }
        public string CatagoryName { get; set; }

        public bool IsActive { get; set; }
    }

}
