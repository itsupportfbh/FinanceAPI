// Finance/AR/ArInvoiceDtos.cs
using System;

namespace UnityWorksERP.Finance.AR
{
    public class ArInvoiceListDto
    {
        public string RowType { get; set; }
        public int InvoiceId { get; set; }
        public string InvoiceNo { get; set; } = string.Empty;
        public DateTime InvoiceDate { get; set; }
        public DateTime? DueDate { get; set; }   // for now = InvoiceDate

        public int CustomerId { get; set; }
        public string CustomerName { get; set; } = string.Empty;

        public decimal Amount { get; set; }        // invoice amount
        public decimal Paid { get; set; }          // receipts
        public decimal CreditNote { get; set; }    // total CN applied to this invoice
        public decimal Outstanding { get; set; }   // Amount - Paid - CreditNote

        public decimal CustomerCreditNoteAmount { get; set; }
        public string? CustomerCreditNoteNo { get; set; }
        public DateTime? CustomerCreditNoteDate { get; set; }
        public byte CustomerCreditStatus { get; set; }
        public string? ReferenceNo { get; set; }
    }
}
