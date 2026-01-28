// ModelDTO/SalesInvoiceDtos.cs
namespace FinanceApi.ModelDTO
{
    public static class SalesInvoiceDtos
    {
        public class SiHeaderDto
        {
            public int Id { get; set; }
            public string InvoiceNo { get; set; } = "";
            public DateTime InvoiceDate { get; set; }
            public byte SourceType { get; set; }
            public int? SoId { get; set; }
            public int? DoId { get; set; }
            public decimal TaxAmount { get; set; }
            public decimal Total { get; set; }
            public byte Status { get; set; }
            public bool IsActive { get; set; }
            public string SourceRef { get; set; } = "";

            // ✅ Print extra fields
            public int? CustomerId { get; set; }
            public string CustomerName { get; set; } = "";
            public string ContactNumber { get; set; } = "";
            public string PointOfContactPerson { get; set; } = "";
            public string Email { get; set; } = "";

            public int? PaymentTermId { get; set; }     // from Customer
            public string PaymentTermsName { get; set; } = ""; // from PaymentTerms table (if exists)

            public int? CurrencyId { get; set; }        // from Quotation
            public string CurrencyName { get; set; } = ""; // from Currency table (if exists)
            public decimal FxRate { get; set; }         // from Quotation
            public DateTime? DeliveryDate { get; set; } // from Quotation (optional)

            public string? DeliveryTo { get; set; }     // from Quotation (optional)
            public string? Remarks { get; set; }        // invoice remarks
        }


        public class SiLineDto
        {
            public int Id { get; set; }
            public int SiId { get; set; }
            public byte SourceType { get; set; }
            public int? SourceLineId { get; set; }
            public int? ItemId { get; set; }
            public string? ItemName { get; set; }
            public string? Uom { get; set; }
            public decimal TaxAmount { get; set; }
            public decimal Qty { get; set; }
            public decimal UnitPrice { get; set; }
            public decimal DiscountPct { get; set; }
            public decimal GstPct { get; set; }
            public string? Tax { get; set; }
            public int? TaxCodeId { get; set; }
            public string? Description { get; set; }   // <— NEW
            public int BudgetLineId {  get; set; }
            public SiLineDto() { }
        }

        public class SiListRowDto
        {
            public int Id { get; set; }
            public string InvoiceNo { get; set; } = "";
            public DateTime InvoiceDate { get; set; }
            public byte SourceType { get; set; }
            public string SourceRef { get; set; } = "";
            public decimal Total { get; set; }
            public SiListRowDto() { }
        }

        public class SiCreateLine
        {
            public int? SourceLineId { get; set; }
            public int ItemId { get; set; }
            public string? ItemName { get; set; }
            public string? Uom { get; set; }
            public decimal Qty { get; set; }
            public decimal UnitPrice { get; set; }
            public decimal DiscountPct { get; set; }
            public decimal GstPct { get; set; }
            public decimal TaxAmount { get; set; }
            public string? Tax { get; set; }
            public int? TaxCodeId { get; set; }
            
            public decimal LineAmount { get; set; }            
            public string? Description { get; set; }   // <— NEW
            public int? BudgetLineId { get; set; }
            public SiCreateLine() { }
        }

        public class SiCreateRequest
        {
            public byte SourceType { get; set; } // 1 or 2
            public int? SoId { get; set; }
            public int? DoId { get; set; }
            public DateTime InvoiceDate { get; set; }
            public decimal? Total { get; set; }
            public decimal Subtotal { get; set; }
            public decimal ShippingCost { get; set; }
            public string Remarks { get; set; }
            public int? AdvanceId { get; set; }
            public decimal TaxAmount { get; set; }
            public decimal? AdvanceApplyAmount { get; set; }

            public List<SiCreateLine> Lines { get; set; } = new();

        }

        public class SiSourceLineDto
        {
            public int SourceLineId { get; set; }
            public byte SourceType { get; set; }
            public int SourceId { get; set; }
            public int ItemId { get; set; }
            public string ItemName { get; set; } = "";
            public string? UomName { get; set; }
            public decimal QtyOpen { get; set; }
            public decimal UnitPrice { get; set; }
            public decimal DiscountPct { get; set; }
            public decimal GstPct { get; set; }
            public string? Tax { get; set; }
            // DefaultCurrencyId intentionally removed
        }
    }
}
