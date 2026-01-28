namespace FinanceApi.Models
{
    public enum CreditNoteStatus : byte { Draft = 1, Approved = 2, Posted = 3 }

    public class CreditNote
    {
        public int Id { get; set; }
        public string CreditNoteNo { get; set; } = "";
        public int DoId { get; set; }
        public string? DoNumber { get; set; }
        public int SiId { get; set; }
        public string? SiNumber { get; set; }
        public int CustomerId { get; set; }
        public string? CustomerName { get; set; } = "";
        public DateTime? CreditNoteDate { get; set; }
        public CreditNoteStatus Status { get; set; } = CreditNoteStatus.Draft;
        public decimal? Subtotal { get; set; }
        public int? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; } = DateTime.UtcNow;
        public int? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; } = true;

        public List<CreditNoteLine> Lines { get; set; } = new();
    }

    public class CreditNoteLine
    {
        public int Id { get; set; }
        public int CreditNoteId { get; set; }
        public int? DoId { get; set; }
        public int? SiId { get; set; }
        public int? DoLineId { get; set; }      
        public int ItemId { get; set; }
        public string ItemName { get; set; } = "";
        public string? Uom { get; set; }
        public decimal DeliveredQty { get; set; }
        public decimal ReturnedQty { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal DiscountPct { get; set; }
        public decimal GstPct { get; set; }
        public string? Tax { get; set; }
        public int? TaxCodeId { get; set; }
        public decimal LineNet { get; set; }
        public int? ReasonId { get; set; }
        public byte? RestockDispositionId { get; set; }
        public int? WarehouseId { get; set; }
        public int? SupplierId { get; set; }
         public int? BinId { get; set; }
        public bool IsActive { get; set; } = true;
    }
}
