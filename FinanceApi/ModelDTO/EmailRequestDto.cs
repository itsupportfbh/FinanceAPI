namespace FinanceApi.ModelDTO
{
    public class EmailInvoiceListItemDto
    {
        public int Id { get; set; }
        public string InvoiceNo { get; set; } = string.Empty;
        public string PartyName { get; set; } = string.Empty; // Customer or Supplier
    }

    public class EmailInvoiceInfoDto
    {
        public int Id { get; set; }
        public string InvoiceNo { get; set; } = string.Empty;
        public string PartyName { get; set; } = string.Empty;
        public string? Email { get; set; }
        public string? CcEmail { get; set; }
        public decimal Amount { get; set; }
    }

    public class EmailTemplateDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string SubjectTemplate { get; set; } = string.Empty;
        public string BodyTemplate { get; set; } = string.Empty;
        public string? DocType { get; set; }  // optional: "SI" or "PIN" or null = both
    }

    public class EmailRequestDto
    {
        public string FromEmail { get; set; } = string.Empty;
        public string FromName { get; set; } = string.Empty;

        public string ToEmail { get; set; } = string.Empty;
        public string ToName { get; set; } = string.Empty;

        public string Subject { get; set; } = string.Empty;
        public string BodyHtml { get; set; } = string.Empty;

        // For attachment file name - e.g. "SI-100013.pdf"
        public string FileName { get; set; } = string.Empty;

        // 👇 NEW: actual invoice number / id used to generate the PDF
        public string InvoiceNo { get; set; }
    }


    public class EmailResultDto
    {
        public bool Success { get; set; }
        public string? Message { get; set; }
    }
    public class SmtpEmailSettings
    {
        public string From { get; set; } = string.Empty;      // default "from" (fallback)
        public string SmtpHost { get; set; } = string.Empty;
        public string SmtpPort { get; set; } = "587";
        public string SmtpUser { get; set; } = string.Empty;
        public string SmtpPass { get; set; } = string.Empty;
    }
    public class InvoicePdfLineDto
    {
        public string ItemName { get; set; } = "";
        public decimal Qty { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal LineTotal { get; set; }
    }

    public class InvoicePdfDto
    {
        public int Id { get; set; }
        public string InvoiceNo { get; set; } = "";
        public DateTime InvoiceDate { get; set; }

        public string CustomerName { get; set; } = "";
        public string CustomerEmail { get; set; } = "";

        public decimal SubTotal { get; set; }
        public decimal TaxAmount { get; set; }
        public decimal GrandTotal { get; set; }

        public List<InvoicePdfLineDto> Lines { get; set; } = new();
    }
}
