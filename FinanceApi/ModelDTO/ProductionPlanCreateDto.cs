namespace FinanceApi.ModelDTO
{
    public class SoHeaderDto
    {
        public int Id { get; set; }
        public string SalesOrderNo { get; set; } = "";
        public int? CustomerId { get; set; }
        public DateTime? DeliveryDate { get; set; }
        public string? Status { get; set; }
    }

    public class PlanRowDto
    {
        public int RecipeId { get; set; }
        public int FinishedItemId { get; set; }
        public string RecipeName { get; set; } = "";
        public decimal PlannedQty { get; set; }
        public decimal ExpectedOutput { get; set; }
        public decimal BatchQty { get; set; }
        public decimal HeaderYieldPct { get; set; }
    }

    public class IngredientRowDto
    {
        public int ItemId { get; set; }
        public string ItemName { get; set; } = "";
        public string? Uom { get; set; }
        public decimal RequiredQty { get; set; }
        public decimal AvailableQty { get; set; }
        public string Status { get; set; } = "OK";
    }

    public class ProductionPlanResponseDto
    {
        public List<PlanRowDto> PlanRows { get; set; } = new();
        public List<IngredientRowDto> Ingredients { get; set; } = new();
    }

    public class SavePlanRequest
    {
        public int SalesOrderId { get; set; }
        public int? OutletId { get; set; }
        public int? WarehouseId { get; set; }
        public string? CreatedBy { get; set; }
    }
    public class ProductionPlanLineDto
    {
        public int Id { get; set; }
        public int ProductionPlanId { get; set; }
        public int RecipeId { get; set; }
        public int? FinishedItemId { get; set; }
        public string? FinishedItemName { get; set; }
        public decimal PlannedQty { get; set; }
        public decimal ExpectedOutput { get; set; }
        public decimal TotalShortage { get; set; }
    }

    public class ProductionPlanListDto
    {
        public int Id { get; set; }
        public int SalesOrderId { get; set; }
        public string SalesOrderNo { get; set; } = "";
        public DateTime PlanDate { get; set; }
        public string? Status { get; set; }
        public string? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public decimal TotalShortage { get; set; }
        public string ProductionPlanNo { get; set; }    
        public List<ProductionPlanLineDto> Lines { get; set; } = new();
    }
    public class ProductionPlanLineUpsertDto
    {
        public int RecipeId { get; set; }
        public int FinishedItemId { get; set; }
        public decimal PlannedQty { get; set; }
        public decimal ExpectedOutput { get; set; }
    }

    public class ProductionPlanUpdateRequest
    {
        public int Id { get; set; }
        public int SalesOrderId { get; set; }
        public int OutletId { get; set; }
        public int WarehouseId { get; set; }
        public DateTime PlanDate { get; set; }
        public string Status { get; set; } = "Draft";
        public string UpdatedBy { get; set; } = "";
        public List<ProductionPlanLineUpsertDto> Lines { get; set; } = new();
    }

    public class ProductionPlanHeaderDto
    {
        public int Id { get; set; }
        public int SalesOrderId { get; set; }
        public int OutletId { get; set; }
        public int WarehouseId { get; set; }
        public DateTime PlanDate { get; set; }
        public string Status { get; set; } = "";
        public string? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
    }

 

    public class ProductionPlanGetByIdDto
    {
        public ProductionPlanHeaderDto Header { get; set; } = new();
        public List<ProductionPlanLineDto> Lines { get; set; } = new();
    }


}
