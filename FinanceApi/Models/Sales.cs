using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    public class Sales : BaseEntity
    {
        public int Id { get; set; }
        public int CustomerId { get; set; }

        [ForeignKey("CustomerId")]
        public Customer? Customer { get; set; }
        public DateTime Date { get; set; }
        public string PaymentAccount { get; set; }

        public List<SaleLineItems> LineItems { get; set; } = new();

        public decimal GrandTotal { get; set; }
        public decimal Discount { get; set; }
        public decimal TotalDiscount { get; set; }
        public decimal NoTax { get; set; }
        public int? GstPercentage { get; set; }
        public decimal? GST { get; set; }
        public decimal TotalTax { get; set; }
        public decimal ShippingCost { get; set; }
        public decimal NetTotal { get; set; }
        public decimal PaidAmount { get; set; }
        public decimal Due { get; set; }
        public decimal Change { get; set; }
        public string Details { get; set; }
    }

    public class SalesDto
    {
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public string? CustomerName { get; set; }
        public DateTime Date { get; set; }
        public string PaymentAccount { get; set; }
        public decimal GrandTotal { get; set; }
        public decimal Discount { get; set; }
        public decimal TotalDiscount { get; set; }
        public decimal NoTax { get; set; }
        public int? GstPercentage { get; set; }
        public decimal? GST { get; set; }
        public decimal TotalTax { get; set; }
        public decimal ShippingCost { get; set; }
        public decimal NetTotal { get; set; }
        public decimal PaidAmount { get; set; }
        public decimal Due { get; set; }
        public decimal Change { get; set; }
        public string Details { get; set; }

        public string? CreatedBy { get; set; }
        public DateTime? CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; }
        public List<SaleLineItems> LineItems { get; set; } = new();
    }

}
