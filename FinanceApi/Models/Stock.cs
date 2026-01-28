namespace FinanceApi.Models
{
    public class Stock
    {
        public int Id { get; set; }
        public int ItemId { get; set; }

        public int Available { get; set; }
        public int OnHand { get; set; }

        public long CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }

        public long? UpdatedBy { get; set; }   // Nullable
        public DateTime? UpdatedDate { get; set; } // Nullable

        public int FromWarehouseID { get; set; }
        public int? ToWarehouseID { get; set; } // Nullable

        public bool IsApproved { get; set; } // Changed to bool

        public string FromWarehouseName { get; set; } = string.Empty;
        public string ItemName { get; set; } = string.Empty;
        public string Sku { get; set; } = string.Empty;
        public int? BinId { get; set; }
        public string? BinName { get; set; }

        public string Remarks { get; set; }

        public int SupplierId { get; set; }
        public bool IsSupplierBased { get; set; }

        public int ToBinId {  get; set; }

        public int? MrId { get; set; }

        public int Status { get; set; }
    }
}
