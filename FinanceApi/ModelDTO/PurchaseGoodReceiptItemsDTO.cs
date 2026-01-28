namespace FinanceApi.ModelDTO
{
    public class PurchaseGoodReceiptItemsDTO
    {

        public int ID { get; set; }
        public int POID { get; set; }

        public string GrnNo { get; set; }

        public DateTime ReceptionDate { get; set; }

        public int OverReceiptTolerance { get; set; }

        public string GRNJson { get; set; }

        public bool isActive { get; set; } = true;
        public string PoLines { get; set; }
        public int CurrencyId { get; set; }
        public decimal Tax { get; set; }
        public string SupplierName {get; set; }
        public int SupplierId { get; set; }
    }
}
