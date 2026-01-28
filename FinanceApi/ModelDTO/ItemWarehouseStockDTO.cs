namespace FinanceApi.ModelDTO
{
    public class ItemWarehouseStockDTO
    {
        public long Id { get; set; }
        public int ItemId { get; set; }

        public int WarehouseId { get; set; }
        public string WarehouseName { get; set; } = "";

        public int BinId { get; set; }
        public string? BinName { get; set; }

        public int? StrategyId { get; set; }

        public decimal OnHand { get; set; }
        public decimal Reserved { get; set; }
        public decimal MinQty { get; set; }
        public decimal MaxQty { get; set; }
        public decimal ReorderQty { get; set; }
        public int? LeadTimeDays { get; set; }

        public bool BatchFlag { get; set; }
        public bool SerialFlag { get; set; }

        public decimal Available { get; set; }
        public decimal Price { get; set; }
        public int SupplierId {  get; set; }
        public string SupplierName { get; set; }
        public string Action { get; set; } = "";
        public DateTime OccurredAtUtc { get; set; }
        public string UserName { get; set; }    
        public string Barcode {  get; set; }   
        public string UserId {  get; set; }
        public string OldValuesJson {  get; set; }
        public string NewValuesJson { get; set; }
        public int AuditId { get; set; }

        public int Qty { get; set; }

        public decimal? CountedQty { get; set; }
        public decimal? BadCountedQty { get; set; }
        public int? ReasonId { get; set; }


    }
}
