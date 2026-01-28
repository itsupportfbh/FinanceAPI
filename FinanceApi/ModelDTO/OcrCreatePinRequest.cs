namespace FinanceApi.ModelDTO
{
    public class OcrCreatePinRequest1
    {
        public IFormFile File { get; set; } = default!;
        public string? Lang { get; set; } = "eng";

        // must have to insert PIN
        public int GrnId { get; set; }
        public int SupplierId { get; set; }
        public int? CurrencyId { get; set; }

        public string? RefNo { get; set; }      // GRN No optional
        public string CreatedBy { get; set; } = "system";
    }
}
