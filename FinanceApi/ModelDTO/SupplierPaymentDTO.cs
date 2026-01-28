namespace FinanceApi.ModelDTO
{
    public class SupplierPaymentDTO
    {
        public int Id { get; set; }
        public string PaymentNo { get; set; }
        public int SupplierId { get; set; }
        public string SupplierName { get; set; }
        public int SupplierInvoiceId { get; set; }
        public string InvoiceNo { get; set; }
        public DateTime PaymentDate { get; set; }
        public int? PaymentMethodId { get; set; }
        public string PaymentMethodName { get; set; }
        public string ReferenceNo { get; set; }
        public decimal Amount { get; set; }
        public string Notes { get; set; }
        public int Status { get; set; }      // 1=Posted
    }

    public class SupplierPaymentCreateDTO
    {
        public int SupplierId { get; set; }
        public int SupplierInvoiceId { get; set; }
        public DateTime PaymentDate { get; set; }
        public int? PaymentMethodId { get; set; }
        public string ReferenceNo { get; set; }
        public decimal Amount { get; set; }
        public string Notes { get; set; }
        public int? BankId { get; set; }

        public int CreatedBy { get; set; }
    }
}
