namespace FinanceApi.ModelDTO
{
    public class ItemMasterUpsertDto
    {
        public long Id { get; set; }              // 0 for create

        // ✅ NEW (use these for Item table style)
        public string ItemCode { get; set; } = "";
        public string ItemName { get; set; } = "";

        // ✅ NEW (SALES / PURCHASE / BOTH)
        public int ItemTypeId { get; set; }

        // ✅ NEW (ids)
        public int CategoryId { get; set; }
        public int UomId { get; set; }
        public int BudgetLineId { get; set; }

        // --------------------------
        // (optional) keep old fields if still used elsewhere
        // --------------------------
        public string Sku { get; set; } = "";
        public string Name { get; set; } = "";
        public string? Category { get; set; }
        public string? Uom { get; set; }

        public long? CostingMethodId { get; set; }
        public long? TaxCodeId { get; set; }
        public string? Specs { get; set; }
        public string? PictureUrl { get; set; }
        public bool IsActive { get; set; } = true;
        public string? CreatedBy { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime ExpiryDate { get; set; }
        public bool HeaderOnly { get; set; } = false;

        public List<ItemPriceDto> Prices { get; set; } = new();
        public List<ItemStockDto> ItemStocks { get; set; } = new();
        public List<ItemBomUpsertDto>? BomLines { get; set; }
    }

}
