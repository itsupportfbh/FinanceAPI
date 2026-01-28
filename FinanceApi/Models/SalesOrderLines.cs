namespace FinanceApi.Models
{
    public class SalesOrderLines : BaseEntity
    {
        public int Id { get; set; }
        public int SalesOrderId { get; set; }
        public int ItemId { get; set; }
        public string ItemName { get; set; } = "";
        public string? Uom { get; set; }
        public decimal Quantity { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal Discount { get; set; }
        public string? Tax { get; set; }
        public int? TaxCodeId { get; set; }
        public decimal Total { get; set; }
        public decimal? LockedQty { get; set; }

        /* From UI */
        public int? SelectedWarehouseId { get; set; }

        public string Description { get; set; }

        /* Stored (computed on server) */
        public int? WarehouseId { get; set; }
        public int? BinId { get; set; }
        public decimal? Available { get; set; }
        public int? SupplierId { get; set; }
    }
}
