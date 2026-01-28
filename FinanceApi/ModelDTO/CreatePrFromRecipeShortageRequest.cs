public class CreatePrFromRecipeShortageRequest
{
    public int SalesOrderId { get; set; }
    public int WarehouseId { get; set; }
    public int OutletId { get; set; }

    public int UserId { get; set; }
    public string UserName { get; set; } = "";

    public DateTime? DeliveryDate { get; set; }   // optional
    public string? Note { get; set; }
}



public class PrLineUiDto
{
    public string itemSearch { get; set; } = "";
    public string itemCode { get; set; } = "";
    public decimal qty { get; set; }
    public string uomSearch { get; set; } = "";
    public string uom { get; set; } = "";
    public string locationSearch { get; set; } = "";
    public string location { get; set; } = "";
    public string budget { get; set; } = "";
    public string remarks { get; set; } = "";
}

public class RecipeShortageRowDto
{
    public long IngredientItemId { get; set; }
    public string IngredientItemName { get; set; } = ""; // ItemMaster.Name
    public string ItemCode { get; set; } = "";           // ItemMaster.Sku
    public string Uom { get; set; } = "";                // ItemMaster.Uom
    public decimal RequiredQty { get; set; }
    public decimal AvailableQty { get; set; }
    public decimal ShortageQty { get; set; }
}

