// ModelDTO/DraftLineDTO.cs
namespace FinanceApi.ModelDTO
{
    public class DraftLineDTO
    {
        public int SalesOrderId { get; set; }
        public string SalesOrderNo { get; set; } = "";
        public int LineId { get; set; }
        public int ItemId { get; set; }
        public string? Tax { get; set; }
        public int? TaxCodeId { get; set; }
        public string ItemName { get; set; } = "";
        public string? Uom { get; set; }
        public decimal Quantity { get; set; }
        public decimal? UnitPrice { get; set; }
        public int? WarehouseId { get; set; }
        public int? BinId { get; set; }
        public int? SupplierId { get; set; }
        public decimal? LockedQty { get; set; }
        public DateTime CreatedDate { get; set; }
        public string Reason { get; set; } = "Warehouse and Supplier is not in the item";
    }
}
