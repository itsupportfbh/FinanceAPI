namespace FinanceApi.Models
{
    public class PurchaseGoodReceiptItemsViewInfo
    {
        public int ID { get; set; }
        public DateTime? ReceptionDate { get; set; } 
        public int? POID { get; set; }
        public string GrnNo { get; set; }

        public string ItemCode { get; set; }
        public string ItemName { get; set; }

        public int? SupplierId { get; set; }
        public string Name { get; set; }

        public string PONO { get; set; }
        public string StorageType { get; set; }
        public string SurfaceTemp { get; set; }        // or decimal? if you make it numeric in SQL
        public DateTime? Expiry { get; set; }
        public string PestSign { get; set; }
        public string DrySpillage { get; set; }
        public string Odor { get; set; }
        public string PlateNumber { get; set; }
        public string DefectLabels { get; set; }
        public string DamagedPackage { get; set; }
        public DateTime? Time { get; set; }
        public string Initial { get; set; }

        public bool isFlagIssue { get; set; }
        public bool isPostInventory { get; set; }

        public int qtyReceived { get; set; }

        public string qualityCheck { get; set; }

        public string batchSerial { get; set; }
        public int warehouseId { get; set; }
        public int binId { get; set; }
        public int strategyId { get; set; }
        public string warehouseName { get; set; }
        public string binName { get; set; }
        public string strategyName { get; set; }
    }
}
