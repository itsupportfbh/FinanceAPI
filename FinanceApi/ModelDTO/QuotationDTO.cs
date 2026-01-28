namespace FinanceApi.ModelDTO
{
    public enum QuotationStatus : byte
    {
        Draft = 0,
        Submitted = 1,
        Approved = 2,
        Rejected = 3,
        Posted = 4
    }

    // ModelDTO/QuotationLineDTO.cs
    public class QuotationLineDTO
    {
        public int Id { get; set; }
        public int QuotationId { get; set; }

        public int ItemId { get; set; }
        public string? ItemName { get; set; }

        public int UomId { get; set; }
        public string? UomName { get; set; }

        public decimal Qty { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal DiscountPct { get; set; }

        public int? TaxCodeId { get; set; }
        public string TaxMode { get; set; } = "EXCLUSIVE";
        public string? TaxCodeLabel { get; set; }

        public decimal? LineNet { get; set; }
        public decimal? LineTax { get; set; }
        public decimal? LineTotal { get; set; }

        // ✅ NEW COLUMN
        public string? Description { get; set; }
    }

    public class QuotationDTO
    {
        public int? Id { get; set; }
        public string Number { get; set; } = "";
        public QuotationStatus Status { get; set; } = QuotationStatus.Draft;

        public int CustomerId { get; set; }
        public string? CustomerName { get; set; }

        public int CurrencyId { get; set; }
        public decimal FxRate { get; set; } = 1m;

        public int PaymentTermsId { get; set; }

        // ✅ ValidityDate -> DeliveryDate
        public DateTime? DeliveryDate { get; set; }

        public decimal Subtotal { get; set; }
        public decimal TaxAmount { get; set; }
        public decimal Rounding { get; set; }
        public decimal GrandTotal { get; set; }
        public bool NeedsHodApproval { get; set; }

        public string? Remarks { get; set; }
        public string? DeliveryTo { get; set; }

        public List<QuotationLineDTO> Lines { get; set; } = new();
    }

    public class QuotationListDTO
    {
        public int? Id { get; set; }
        public string Number { get; set; } = "";
        public QuotationStatus Status { get; set; } = QuotationStatus.Draft;

        public int CustomerId { get; set; }
        public string? CustomerName { get; set; }

        public string ItemCode { get; set; }

        public int CurrencyId { get; set; }
        public decimal FxRate { get; set; } = 1m;

        public int PaymentTermsId { get; set; }

        // ✅ ValidityDate -> DeliveryDate
        public DateTime? DeliveryDate { get; set; }

        public decimal Subtotal { get; set; }
        public decimal TaxAmount { get; set; }
        public decimal Rounding { get; set; }
        public decimal GrandTotal { get; set; }
        public bool NeedsHodApproval { get; set; }

        public string CurrencyName { get; set; } = "";
        public string PaymentTermsName { get; set; } = "";

        public string? Remarks { get; set; }
        public string? DeliveryTo { get; set; }

        public string CreatedUserName { get; set; }

        public string ModifiedUserName { get; set; }
        public List<QuotationLineDTO> Lines { get; set; } = new();
    }
}
