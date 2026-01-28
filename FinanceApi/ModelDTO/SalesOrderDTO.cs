using FinanceApi.Models;

namespace FinanceApi.ModelDTO
{
    public class SalesOrderDTO
    {
        public int Id { get; set; }
        public int QuotationNo { get; set; }
        public int CustomerId { get; set; }
        public string Number {  get; set; }
        public string? CustomerName { get; set; }

        public DateTime? RequestedDate { get; set; }
        public DateTime? DeliveryDate { get; set; }
        public byte Status { get; set; } = 1;

        public decimal Shipping { get; set; }
        public decimal Discount { get; set; }
        public decimal GstPct { get; set; }

        public int? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; }
        public int? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; } = true;

        public string? SalesOrderNo { get; set; }

        public decimal TaxAmount { get; set; }
        public string DeliveryTo { get; set; }
        public List<SalesOrderLineDTO> LineItems { get; set; } = new();
    }
}
