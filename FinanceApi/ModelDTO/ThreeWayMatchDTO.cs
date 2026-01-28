namespace FinanceApi.ModelDTO
{
    public class ThreeWayMatchDTO
    {
        public int PoId { get; set; }
        public string PoNo { get; set; } = string.Empty;
        public decimal PoQty { get; set; }
        public decimal PoPrice { get; set; }
        public decimal PoTotal { get; set; }

        public int GrnId { get; set; }
        public string GrnNo { get; set; } = string.Empty;
        public decimal GrnReceivedQty { get; set; }
        public decimal GrnVarianceQty { get; set; }
        public string GrnStatus { get; set; } = string.Empty;

        public int PinId { get; set; }
        public string PinNo { get; set; } = string.Empty;
        public decimal PinQty { get; set; }
        public decimal PinTotal { get; set; }

        public bool PinMatch { get; set; }
    }
}
