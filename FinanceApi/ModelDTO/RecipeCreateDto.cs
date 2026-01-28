namespace FinanceApi.ModelDTO
{
    public class RecipeCreateDto
    {
        public int FinishedItemId { get; set; }
        public string? Cuisine { get; set; }
        public string Status { get; set; } = "Draft";
        public decimal YieldPct { get; set; } = 100;
        public decimal BatchQty { get; set; } = 0;
        public string? BatchUom { get; set; }
        public string? Notes { get; set; }

        public List<RecipeIngredientDto> Ingredients { get; set; } = new();
    }

    public class RecipeUpdateDto
    {
        public int FinishedItemId { get; set; }
        public string? Cuisine { get; set; }
        public string Status { get; set; } = "Draft";
        public decimal YieldPct { get; set; } = 100;
        public decimal BatchQty { get; set; } = 0;
        public string? BatchUom { get; set; }
        public string? Notes { get; set; }

        public List<RecipeIngredientDto> Ingredients { get; set; } = new();
    }

    public class RecipeIngredientDto
    {
        public int IngredientItemId { get; set; }
        public decimal Qty { get; set; }
        public string? Uom { get; set; }          // send uomName here
        public decimal YieldPct { get; set; } = 100;
        public decimal UnitCost { get; set; } = 0;
        public int SortOrder { get; set; } = 1;
        public string? Remarks { get; set; }
    }

    public class RecipeListDto
    {
        public int Id { get; set; }
        public int FinishedItemId { get; set; }
        public string FinishedItemCode { get; set; } = "";
        public string FinishedItemName { get; set; } = "";
        public string Status { get; set; } = "";
        public string? Cuisine { get; set; }
        public decimal TotalCost { get; set; }
        public DateTime CreatedDate { get; set; }
    }

    public class RecipeReadDto
    {
        public int Id { get; set; }
        public int FinishedItemId { get; set; }
        public string FinishedItemCode { get; set; } = "";
        public string FinishedItemName { get; set; } = "";
        public string FinishedUomName { get; set; } = "";

        public string Status { get; set; } = "Draft";
        public string? Cuisine { get; set; }
        public decimal YieldPct { get; set; }
        public decimal BatchQty { get; set; }
        public string? BatchUom { get; set; }
        public string? Notes { get; set; }

        public decimal TotalCost { get; set; }
        public decimal ExpectedOutput { get; set; }
        public decimal CostPerUnit { get; set; }

        public List<RecipeIngredientReadDto> Ingredients { get; set; } = new();
    }

    public class RecipeIngredientReadDto
    {
        public int Id { get; set; }
        public int IngredientItemId { get; set; }
        public string IngredientItemCode { get; set; } = "";
        public string IngredientItemName { get; set; } = "";
        public string IngredientUomName { get; set; } = "";

        public decimal Qty { get; set; }
        public string? Uom { get; set; }
        public decimal YieldPct { get; set; }
        public decimal UnitCost { get; set; }
        public decimal RowCost { get; set; }
        public int SortOrder { get; set; }
        public DateTime CreatedDate { get; set; }
        public string? Remarks { get; set; }
    }
}
