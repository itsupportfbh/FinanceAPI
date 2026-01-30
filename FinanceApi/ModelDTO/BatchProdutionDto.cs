public class BatchProductionLineDto
{
    public int Id { get; set; }
    public int BatchProductionId { get; set; }
    public int RecipeId { get; set; }
    public int? FinishedItemId { get; set; }
    public decimal PlannedQty { get; set; }
    public decimal ActualQty { get; set; }
    public string? RecipeName { get; set; }          // optional view field
    public string? FinishedItemName { get; set; }    // optional view field
}

public class BatchProductionHeaderDto
{
    public int Id { get; set; }
    public int ProductionPlanId { get; set; }
    public int WarehouseId { get; set; }
    public string? BatchNo { get; set; }
    public string Status { get; set; } = "Draft";
    public string? CreatedBy { get; set; }
    public DateTime CreatedDate { get; set; }
    public string? UpdatedBy { get; set; }
    public DateTime? UpdatedDate { get; set; }
    public string? PostedBy { get; set; }
    public DateTime? PostedDate { get; set; }
    public string? ProductionPlanNo { get; set; }
    public string? Name { get; set; }
    public DateTime? PlanDate { get; set; }
}

public class BatchProductionGetByIdDto
{
    public BatchProductionHeaderDto Header { get; set; } = new();
    public List<BatchProductionLineDto> Lines { get; set; } = new();
}

public class BatchProductionSaveRequest
{
    public int? Id { get; set; }
    public int ProductionPlanId { get; set; }

    // ✅ UI doesn't need to send. Keep optional if you want.
    public int WarehouseId { get; set; }

    public string? BatchNo { get; set; }
    public int UserId { get; set; }
    public List<BatchProductionLineDto> Lines { get; set; } = new();
}
