namespace FinanceApi.ModelDTO
{
    public class SalesOrderLineDTO
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

        /* New (read-back) */
        public int? WarehouseId { get; set; }
        public int? BinId { get; set; }
        public decimal? Available { get; set; }
        public int? SupplierId { get; set; }


        public string? WarehouseName { get; set; }
        public string? Bin { get; set; }

        public string? SupplierName { get; set; }
        public string? Description { get; set; }

        public decimal TaxAmount { get; set; }
        public List<SalesOrderLineAllocDTO>? Allocations { get; set; }

    }
    public class SalesOrderListDto
    {
        public int Id { get; set; }
        public string SalesOrderNo { get; set; } = "";
        public DateTime DeliveryDate { get; set; }
        public int CustomerId { get; set; }
        public decimal GrandTotal { get; set; }
        public int Status { get; set; }
    }

}
