namespace FinanceApi.ModelDTO
{
    public class ApMatchDTO
    {
        public string PoNo { get; set; } = "";
        public string GrnNo { get; set; } = "";
        public string InvoiceNo { get; set; } = "";
        public string SupplierName { get; set; } = "";
        public decimal PoAmount { get; set; }
        public decimal InvoiceAmount { get; set; }
        public string Status { get; set; } = "";
    }
}
