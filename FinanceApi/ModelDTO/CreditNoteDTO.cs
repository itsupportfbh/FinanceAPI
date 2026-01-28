namespace FinanceApi.ModelDTO
{
    public class CreditNoteDTO
    {
        public int Id { get; set; }
        public string CreditNoteNo { get; set; } = "";
        public int DoId { get; set; }
        public string? DoNumber { get; set; }
        public int? SiId { get; set; }
        public string? SiNumber { get; set; }
        public int CustomerId { get; set; }
        public string CustomerName { get; set; } = "";
        public DateTime CreditNoteDate { get; set; }
        public byte Status { get; set; }
        public decimal Subtotal { get; set; }
        public bool IsActive { get; set; }

        public List<CreditNoteLineDTO> Lines { get; set; } = new();
    }

    public class CreditNoteLineDTO
    {
        public int Id { get; set; }
        public int CreditNoteId { get; set; }
        public int? DId { get; set; }
        public int? SiId { get; set; }
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
        public bool IsActive { get; set; }
    }
}
