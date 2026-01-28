namespace FinanceApi.ModelDTO
{
    // ModelDTO/PurchaseAlertDTO.cs
    public class PurchaseAlertDTO
    {
        public int Id { get; set; }
        public int ItemId { get; set; }
        public string ItemName { get; set; }
        public decimal RequiredQty { get; set; }
        public int WarehouseId { get; set; }
        public int SupplierId { get; set; }

        public string WarehouseName { get; set; }   // ✅ NEW
        public string SupplierName { get; set; }    // ✅ NEW

        public string Source { get; set; }
        public int SourceId { get; set; }
        public string SourceNo { get; set; }
        public string Message { get; set; }
        public bool IsRead { get; set; }
        public DateTime CreatedDate { get; set; }
    }


}
