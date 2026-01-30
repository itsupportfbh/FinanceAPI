public class IngredientExplosionRowDto
{
    public int IngredientItemId { get; set; }
    public string IngredientName { get; set; } = "";
    public decimal RequiredQty { get; set; }
    public decimal AvailableQty { get; set; }
    public string Status { get; set; } = "OK"; // OK / Shortage
}
