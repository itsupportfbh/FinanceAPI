namespace FinanceApi.ModelDTO
{
    public class SupplierInvoicePinDTO
    {
        public int Id { get; set; }
        public string InvoiceNo { get; set; } = "";
        public DateTime InvoiceDate { get; set; }
        public decimal Amount { get; set; }
        public decimal Tax { get; set; }
        public int CurrencyId { get; set; }
        public int Status { get; set; }            // 0 draft,1 hold,2 posted
        public string? LinesJson { get; set; }     // 👈 JSON from UI
        public bool IsActive { get; set; } = true;
        public DateTime CreatedDate { get; set; }
        public DateTime UpdatedDate { get; set; }
        public string? CreatedBy { get; set; }
        public string? UpdatedBy { get; set; }
        public int? GrnId { get; set; }
        public string GrnNo { get; set; }
        public string SupplierName { get; set; }    
        public int SupplierId { get; set; }
        
    }
}
