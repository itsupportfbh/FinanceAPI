namespace FinanceApi.ModelDTO
{
    public class ApInvoiceDTO
    {
        public int Id { get; set; }
        public int SupplierId { get; set; }
        public string SupplierName { get; set; } = "";
        public string InvoiceNo { get; set; } = "";
        public DateTime InvoiceDate { get; set; }
        public DateTime? DueDate { get; set; }

        public decimal GrandTotal { get; set; }
        public decimal PaidAmount { get; set; }
        public decimal DebitNoteAmount { get; set; }
        public string DebitNoteNo { get; set; } = "";
        public DateTime? DebitNoteDate { get; set; }
        public decimal OutstandingAmount { get; set; }
        public int Status { get; set; }
    }
    public class ApPaymentCreateDto
    {
        public int SupplierInvoiceId { get; set; }
        public int SupplierId { get; set; }
        public DateTime PaymentDate { get; set; }
        public int PaymentMethodId { get; set; }
        public string ReferenceNo { get; set; } = "";
        public decimal Amount { get; set; }
        public string Notes { get; set; } = "";
        public int? BankId { get; set; }
    }
    public class AccountingPeriodRow
    {
        public int Id { get; set; }
        public string PeriodName { get; set; } = "";
        public bool IsLocked { get; set; }
    }
    public class ApPaymentListDto
    {
        public int Id { get; set; }
        public string PaymentNo { get; set; } = "";
        public string SupplierName { get; set; } = "";
        public string InvoiceNo { get; set; } = "";
        public DateTime PaymentDate { get; set; }
        public int PaymentMethodId { get; set; }
        public string PaymentMethodName { get; set; } = "";
        public decimal Amount { get; set; }
        public string ReferenceNo { get; set; } = "";
        public string Notes { get; set; } = "";
    }
}
