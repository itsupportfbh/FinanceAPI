namespace FinanceApi.ModelDTO
{
    public class ItemMasterDTO
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
        public DateTime? ExpiryDate { get; set; }

        public int Qty { get; set; }
    }
    public class getItemMasterDTO
    {
        public long Id { get; set; }
        public long? ItemId { get; set; }              // ✅ link to Item.Id

        public string Sku { get; set; } = "";
        public string Name { get; set; } = "";

        // Existing (stored as text in ItemMaster)
        public string? Category { get; set; }
        public string? Uom { get; set; }

        // ✅ NEW: from Item table
        public string? ItemTypeId { get; set; }          // SALES / PURCHASE / BOTH
        public int? CategoryId { get; set; }
        public int? UomId { get; set; }
        public int? BudgetLineId { get; set; }
        public string? BudgetLineName { get; set; }

        public long? CostingMethodId { get; set; }
        public long? TaxCodeId { get; set; }
        public string? Specs { get; set; }
        public string? PictureUrl { get; set; }
        public bool IsActive { get; set; } = true;

        // inventory join
        public decimal OnHand { get; set; }
        public decimal Reserved { get; set; }
        public decimal Available { get; set; }

        public DateTime? ExpiryDate { get; set; }
        public int Qty { get; set; }
        public string ItemTypeName { get; set; }
    }

}
