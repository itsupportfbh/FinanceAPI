namespace UnityWorksERP.Finance.AR
{
    public class SalesInvoiceOpenDto
    {
        public int Id { get; set; }
        public string InvoiceNo { get; set; } = string.Empty;
        public DateTime InvoiceDate { get; set; }
        public decimal Amount { get; set; }
        public decimal PaidAmount { get; set; }
        public decimal Balance { get; set; }
    }

    // For list page (Receipts tab)
    public class ArReceiptListDto
    {
        public int Id { get; set; }
        public string ReceiptNo { get; set; } = string.Empty;
        public int CustomerId { get; set; }
        public string CustomerName { get; set; } = string.Empty;
        public DateTime ReceiptDate { get; set; }
        public string PaymentMode { get; set; } = string.Empty;
        public int? BankId { get; set; }
        public string BankName { get; set; } = string.Empty;
        public decimal AmountReceived { get; set; }
        public decimal TotalAllocated { get; set; }
        public decimal Unallocated { get; set; }
        public string Status { get; set; } = "Posted";
        public string InvoiceNos { get; set; } = "";
    }

    // Line DTO for detail
    public class ArReceiptAllocationDto
    {
        public int Id { get; set; }
        public int InvoiceId { get; set; }
        public string InvoiceNo { get; set; } = string.Empty;
        public DateTime InvoiceDate { get; set; }      // NEW
        public decimal Amount { get; set; }            // NEW  (invoice total)
        public decimal PaidAmount { get; set; }        // NEW  (total paid for this invoice)
        public decimal Balance { get; set; }
        public decimal AllocatedAmount { get; set; }
    }

    // Detail DTO (header + lines)
    public class ArReceiptDetailDto
    {
        public int Id { get; set; }
        public string ReceiptNo { get; set; } = string.Empty;
        public int CustomerId { get; set; }
        public string CustomerName { get; set; } = string.Empty;
        public DateTime ReceiptDate { get; set; }
        public string PaymentMode { get; set; } = string.Empty;
        public int? BankId { get; set; }
        public string BankName { get; set; } = string.Empty;
        public decimal AmountReceived { get; set; }
        public decimal TotalAllocated { get; set; }
        public decimal Unallocated { get; set; }
        public string? ReferenceNo { get; set; }
        public string? Remarks { get; set; }
        public string Status { get; set; } = "Posted";

        public List<ArReceiptAllocationDto> Allocations { get; set; } = new();
    }

    // Create/Update payload from Angular
    public class ArReceiptAllocationCreateDto
    {
        public int InvoiceId { get; set; }
        public string InvoiceNo { get; set; } = string.Empty;   // optional, UI only
        public decimal AllocatedAmount { get; set; }
    }

    public class ArReceiptCreateUpdateDto
    {
        public int? Id { get; set; }                            // null for create
        public int CustomerId { get; set; }
        public DateTime ReceiptDate { get; set; }
        public string PaymentMode { get; set; } = "CASH";
        public int? BankId { get; set; }
        public decimal AmountReceived { get; set; }
        public decimal TotalAllocated { get; set; }
        public string? ReferenceNo { get; set; }
        public string? Remarks { get; set; }

        public List<ArReceiptAllocationCreateDto> Allocations { get; set; } = new();
    }
}
