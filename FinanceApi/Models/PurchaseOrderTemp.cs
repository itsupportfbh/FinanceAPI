using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    public class PurchaseOrderTemp : BaseEntity
    {
        public int Id { get; set; }
        public string PurchaseOrderNo { get; set; }
        public int ApprovalStatus { get; set; }
        public int SupplierId { get; set; }
        public int ApproveLevelId { get; set; }
        public int PaymentTermId { get; set; }

        [ForeignKey("PaymentTermId")]
        public PaymentTerms? PaymentTerms { get; set; }
        public int CurrencyId { get; set; }
        public Currency? Currency { get; set; }
        public int IncotermsId { get; set; }
        public DateTime PoDate { get; set; }
        public DateTime DeliveryDate { get; set; }
        public string Remarks { get; set; }
        public decimal FxRate { get; set; }
        public decimal Tax { get; set; }
        public decimal Shipping { get; set; }
        public decimal Discount { get; set; }
        public decimal SubTotal { get; set; }
        public decimal NetTotal { get; set; }
        public string PoLines { get; set; }
        public string? PurchaseRequestNo { get; set; }
    }

    public class PurchaseOrderTempDto
    {
        public int Id { get; set; }
        public string PurchaseOrderNo { get; set; }
        public int ApprovalStatus { get; set; }
        public int SupplierId { get; set; }
        public string? SupplierName { get; set; }
        public int ApproveLevelId { get; set; }
        public string? ApproveLevelName { get; set; }
        public int PaymentTermId { get; set; }
        public string? PaymentTermName { get; set; }
        public int CurrencyId { get; set; }
        public string? CurrencyName { get; set; }
        public int IncotermsId { get; set; }
        public string? IncotermsName { get; set; }
        public DateTime PoDate { get; set; }
        public DateTime DeliveryDate { get; set; }
        public string Remarks { get; set; }
        public decimal FxRate { get; set; }
        public decimal? Tax { get; set; }
        public decimal? Shipping { get; set; }
        public decimal? Discount { get; set; }
        public decimal SubTotal { get; set; }
        public decimal NetTotal { get; set; }
        public string PoLines { get; set; }
        public string? CreatedBy { get; set; }
        public DateTime? CreatedDate { get; set; }
        public string? UpdatedBy { get; set; }
        public DateTime? UpdatedDate { get; set; }
        public bool IsActive { get; set; }
    }
}
