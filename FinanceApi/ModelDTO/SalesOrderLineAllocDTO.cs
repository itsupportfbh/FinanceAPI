namespace FinanceApi.ModelDTO
{
    public class SalesOrderLineAllocDTO
    {
        public int Id { get; set; }
        public int SalesOrderLineId { get; set; }
        public int? WarehouseId { get; set; }
        public string? WarehouseName { get; set; }
        public int? BinId { get; set; }
        public string? BinName { get; set; }
        public int? SupplierId { get; set; }
        public string? SupplierName { get; set; }
        public decimal AllocQty { get; set; }
    }
}
