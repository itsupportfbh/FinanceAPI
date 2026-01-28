using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.ModelDTO
{
    public class OcrParsedDto
    {
        public string? InvoiceNo { get; set; }
        public string? InvoiceDate { get; set; } // yyyy-MM-dd
        public decimal? Total { get; set; }
        public decimal? TaxPercent { get; set; }
        public decimal? TaxAmount { get; set; }
        public decimal? Discount { get; set; }
        public decimal? SubTotal { get; set; }
        public string? SupplierName { get; set; }
    }

    public class OcrResponseDto
    {
        public int OcrId { get; set; }
        public string Text { get; set; } = "";
        public float MeanConfidence { get; set; }
        public int WordCount { get; set; }
        public OcrParsedDto Parsed { get; set; } = new();
    }
    public class OcrExtractRequest
    {
        [FromForm(Name = "file")]
        public IFormFile File { get; set; } = default!;

        [FromForm(Name = "lang")]
        public string? Lang { get; set; }

        [FromForm(Name = "module")]
        public string? Module { get; set; }

        [FromForm(Name = "refNo")]
        public string? RefNo { get; set; }

        [FromForm(Name = "createdBy")]
        public string? CreatedBy { get; set; }
    }
    public class OcrCreatePinRequest
    {
        [FromForm(Name = "file")]
        public IFormFile File { get; set; } = default!;

        [FromForm(Name = "lang")]
        public string? Lang { get; set; }

        // Without GRN => optional
        [FromForm(Name = "currencyId")]
        public int? CurrencyId { get; set; }

        [FromForm(Name = "createdBy")]
        public string? CreatedBy { get; set; }
    }
    public class OcrTextResult
    {
        public string Text { get; set; } = "";
        public float MeanConfidence { get; set; } = 0;
        public int WordCount { get; set; } = 0;
    }
}
