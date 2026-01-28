namespace FinanceApi.ModelDTO
{
    public class SupplierInvoiceDTO
    {
        public int Id { get; set; }
        public int SupplierId { get; set; }
        public string InvoiceNo { get; set; } = string.Empty;
        public DateTime InvoiceDate { get; set; }
        public decimal Amount { get; set; }
        public decimal Tax { get; set; }
        public string? Currency { get; set; }
        public string? MatchResult { get; set; }
        public string? SupplierName { get; set; }   // join
        public IEnumerable<SupplierInvoiceLineDTO>? Lines { get; set; }
    }

    public class SupplierInvoiceLineDTO
    {
        public int Id { get; set; }
        public string? PONo { get; set; }
        public string? GRNNo { get; set; }
        public string? Item { get; set; }
        public decimal? Qty { get; set; }
        public decimal? Price { get; set; }
        public string? DcNoteNo { get; set; }
        public string? MatchStatus { get; set; }
        public string? MismatchFields { get; set; }
    }
}
