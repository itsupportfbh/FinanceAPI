using System.ComponentModel.DataAnnotations.Schema;

namespace FinanceApi.Models
{
    [Table("OcrDocument")]
    public class OcrDocument
    {
        public int Id { get; set; }
        public string? Module { get; set; }
        public string? RefNo { get; set; }

        public string? FileName { get; set; }
        public string? ContentType { get; set; }
        public string? FilePath { get; set; }

        public string? ExtractedText { get; set; }
        public string? ParsedJson { get; set; }

        public decimal? MeanConfidence { get; set; }

        public string? CreatedBy { get; set; }
        public DateTime CreatedDate { get; set; } = DateTime.Now;
    }
}
