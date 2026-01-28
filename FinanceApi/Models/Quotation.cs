using FinanceApi.ModelDTO;
using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    [Table("Quotation")]
    public class Quotation
    {
        public int? Id { get; set; }
        public string Number { get; set; } = "";
        public QuotationStatus Status { get; set; } = QuotationStatus.Draft;

        public int CustomerId { get; set; }
        public string? CustomerName { get; set; }

        public int CurrencyId { get; set; }
        public decimal FxRate { get; set; } = 1m;
        public int PaymentTermsId { get; set; }
        public DateTime? ValidityDate { get; set; }

        public decimal Subtotal { get; set; }
        public decimal TaxAmount { get; set; }
        public decimal Rounding { get; set; }
        public decimal GrandTotal { get; set; }
        public bool NeedsHodApproval { get; set; }
    }
}
