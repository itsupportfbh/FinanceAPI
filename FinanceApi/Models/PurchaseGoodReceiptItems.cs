using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    [Table("PurchaseGoodReceipt")]
    public class PurchaseGoodReceiptItems
    {

        public int ID { get; set; }
        public int POID { get; set; }

        public string GrnNo { get; set; }

        public DateTime ReceptionDate { get; set; }

        public int OverReceiptTolerance { get; set; }

        public string GRNJson { get; set; }

        public bool isActive { get; set; } = true;

        public string? SourceType { get; set; }
        public int? SourceRefId { get; set; }
    }
}
