using FinanceApi.ModelDTO;
using ImageMagick;
using Tesseract;
using UglyToad.PdfPig;
using UglyToad.PdfPig.Content;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Page = UglyToad.PdfPig.Content.Page;

namespace FinanceApi.Services
{
    public interface IOcrService
    {
        Task<OcrResponseDto> ExtractAnyAsync(IFormFile file, string lang, string? module, string? refNo, string? createdBy);
    }
    public class OcrService:IOcrService
    {
        private readonly IWebHostEnvironment _env;
        private readonly IConfiguration _cfg;

        public OcrService(IWebHostEnvironment env, IConfiguration cfg)
        {
            _env = env;
            _cfg = cfg;
        }

        public async Task<OcrResponseDto> ExtractAnyAsync(IFormFile file, string lang, string? module, string? refNo, string? createdBy)
        {
            if (file == null || file.Length == 0) throw new InvalidOperationException("File is empty");

            // save original
            var uploads = Path.Combine(_env.ContentRootPath, "Uploads", "OCR");
            Directory.CreateDirectory(uploads);

            var ext = Path.GetExtension(file.FileName).ToLowerInvariant();
            var safeName = $"{DateTime.Now:yyyyMMdd_HHmmss}_{Guid.NewGuid():N}{ext}";
            var savedPath = Path.Combine(uploads, safeName);

            await using (var fs = new FileStream(savedPath, FileMode.Create))
                await file.CopyToAsync(fs);

            string extractedText = "";
            float conf = 0;
            int wordCount = 0;

            if (ext == ".pdf" || file.ContentType.Contains("pdf", StringComparison.OrdinalIgnoreCase))
            {
                // 1) Try digital PDF text first
                extractedText = ExtractPdfText(savedPath);

                if (CountWords(extractedText) < 20) // scanned pdf likely
                {
                    var dpi = _cfg.GetValue<int?>("Ocr:PdfDpi") ?? 300;
                    var pngs = RenderPdfToPngs(savedPath, dpi);
                    var ocr = RunTesseractOnImages(pngs, lang);
                    extractedText = ocr.Text;
                    conf = ocr.MeanConfidence;
                    wordCount = ocr.WordCount;

                    // cleanup temp pngs
                    foreach (var p in pngs) TryDelete(p);
                }
                else
                {
                    wordCount = CountWords(extractedText);
                    conf = 0.99f; // digital text (not OCR)
                }
            }
            else
            {
                // image
                var ocr = RunTesseractOnImage(savedPath, lang);
                extractedText = ocr.Text;
                conf = ocr.MeanConfidence;
                wordCount = ocr.WordCount;
            }

            var parsed = ParseInvoice(extractedText);

            // NOTE: Here you can store OCR doc to DB and set OcrId from DB.
            // For now return dummy id
            return new OcrResponseDto
            {
                OcrId = 1,
                Text = extractedText,
                MeanConfidence = conf,
                WordCount = wordCount,
                Parsed = parsed
            };
        }

        // ---------------- PDF TEXT (Digital) ----------------
        private string ExtractPdfText(string pdfPath)
        {
            var sb = new StringBuilder();
            using var document = PdfDocument.Open(pdfPath);
            foreach (Page page in document.GetPages())
            {
                sb.AppendLine(page.Text);
                sb.AppendLine();
            }
            return sb.ToString();
        }

        // ---------------- PDF -> PNG (Scanned) ----------------
        private List<string> RenderPdfToPngs(string pdfPath, int dpi)
        {
            // Requires Ghostscript installed + in PATH
            var outDir = Path.Combine(Path.GetTempPath(), "ocr_pdf_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(outDir);

            var outFiles = new List<string>();

            var settings = new MagickReadSettings
            {
                Density = new Density(dpi),
                FrameIndex = 0,
                FrameCount = 0
            };

            using var images = new MagickImageCollection();
            images.Read(pdfPath, settings);

            int i = 0;
            foreach (var img in images)
            {
                img.Alpha(AlphaOption.Remove);
                img.ColorSpace = ColorSpace.sRGB;
                img.Format = MagickFormat.Png;

                var outPath = Path.Combine(outDir, $"page_{++i}.png");
                img.Write(outPath);
                outFiles.Add(outPath);
            }

            return outFiles;
        }

        // ---------------- TESSERACT ----------------
        private OcrTextResult RunTesseractOnImage(string imagePath, string lang)
            => RunTesseractOnImages(new List<string> { imagePath }, lang);

        private OcrTextResult RunTesseractOnImages(List<string> imagePaths, string lang)
        {
            var tessdata = Path.Combine(_env.ContentRootPath, _cfg["Ocr:TessdataRelativePath"] ?? "OCR\\tessdata");

            if (!Directory.Exists(tessdata))
                throw new DirectoryNotFoundException($"tessdata not found: {tessdata}");

            // must contain eng.traineddata etc
            var sb = new StringBuilder();
            float totalConf = 0;
            int totalWords = 0;
            int pageCount = 0;

            using var engine = new TesseractEngine(tessdata, lang, EngineMode.Default);

            foreach (var p in imagePaths)
            {
                using var img = Pix.LoadFromFile(p);
                using var page = engine.Process(img);
                var text = page.GetText() ?? "";
                var conf = page.GetMeanConfidence();

                sb.AppendLine(text);
                sb.AppendLine("\n--- PAGE BREAK ---\n");

                totalConf += conf;
                totalWords += CountWords(text);
                pageCount++;
            }

            return new OcrTextResult
            {
                Text = sb.ToString(),
                MeanConfidence = pageCount == 0 ? 0 : totalConf / pageCount,
                WordCount = totalWords
            };
        }



private OcrParsedDto ParseInvoice(string text)
    {
        var p = new OcrParsedDto();
        if (string.IsNullOrWhiteSpace(text)) return p;

        // normalize
        text = text.Replace("\r", "\n");
        text = Regex.Replace(text, @"[ \t]+", " ");
        text = Regex.Replace(text, @"\n{2,}", "\n").Trim();

        // -----------------------------
        // 1) SUPPLIER (strict)
        // -----------------------------
        // Prefer: "Biopak Pte Ltd" anywhere (because BioPak PDF has tagline before it)
        p.SupplierName =
            FirstGroup(text, @"\b(Biopak\s+Pte\s+Ltd)\b", RegexOptions.IgnoreCase)
            ?? FirstGroup(text, @"\b([A-Za-z][A-Za-z &\.\-]*?\b(?:Pte\s+Ltd|PVT\s+LTD|LTD|Limited))\b", RegexOptions.IgnoreCase);

        // -----------------------------
        // 2) INVOICE NO (strict)
        // -----------------------------
        // BioPak format: INV-S-46891 (INV-<LETTER>-<NUMBER>)
        p.InvoiceNo =
            FirstGroup(text, @"\b(INV-[A-Z]-\d+)\b", RegexOptions.IgnoreCase)
            ?? FirstGroup(text, @"\b(INV[-A-Za-z]*-\d{3,})\b", RegexOptions.IgnoreCase)
            ?? FirstGroup(text, @"Invoice\s*#\s*([A-Za-z0-9\-\/]+)", RegexOptions.IgnoreCase);

        // -----------------------------
        // 3) INVOICE DATE
        // -----------------------------
        // Handles: "DateInvoice #9/12/2025INV-S-46891"
        var dateRaw =
            FirstGroup(text, @"Date\s*Invoice\s*#?\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})", RegexOptions.IgnoreCase)
            ?? FirstGroup(text, @"\bDate\b\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})", RegexOptions.IgnoreCase);

        p.InvoiceDate = ToIsoDate(dateRaw);

        // -----------------------------
        // 4) TOTALS (BioPak bottom section)
        // -----------------------------
        p.SubTotal = FindMoney(text, @"\bSubtotal\b\s*\$?\s*([0-9,]+(?:\.\d{2})?)");
        p.TaxAmount = FindMoney(text, @"Tax\s*Total\s*\(\s*\d{1,2}\s*%\s*\)\s*\$?\s*([0-9,]+(?:\.\d{2})?)");
        p.Total = FindMoneyLast(text, @"\bTotal\b\s*\$?\s*([0-9,]+(?:\.\d{2})?)")
              ?? FindMoneyLast(text, @"Balance\s*Due\b\s*\$?\s*([0-9,]+(?:\.\d{2})?)");

        // -----------------------------
        // 5) TAX PERCENT
        // -----------------------------
        var pct =
            FirstGroup(text, @"Tax\s*Total\s*\((\d{1,2})\%\)", RegexOptions.IgnoreCase)
            ?? FirstGroup(text, @"GST\s*(\d{1,2})\%", RegexOptions.IgnoreCase);

        if (decimal.TryParse(pct, NumberStyles.Number, CultureInfo.InvariantCulture, out var dp))
            p.TaxPercent = dp;

        return p;
    }

    // ---------------- helpers ----------------

    private static string? FirstGroup(string input, string pattern, RegexOptions opt)
    {
        var m = Regex.Match(input ?? "", pattern, opt);
        return m.Success && m.Groups.Count > 1 ? m.Groups[1].Value.Trim() : null;
    }

    private static decimal? FindMoney(string input, string pattern)
    {
        var s = FirstGroup(input, pattern, RegexOptions.IgnoreCase);
        if (string.IsNullOrWhiteSpace(s)) return null;

        s = s.Replace(",", "").Trim();
        if (decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out var v))
            return v;

        return null;
    }

    private static decimal? FindMoneyLast(string input, string pattern)
    {
        var ms = Regex.Matches(input ?? "", pattern, RegexOptions.IgnoreCase);
        if (ms.Count == 0) return null;

        var last = ms[ms.Count - 1].Groups[1].Value.Trim().Replace(",", "");
        if (decimal.TryParse(last, NumberStyles.Number, CultureInfo.InvariantCulture, out var v))
            return v;

        return null;
    }

    private static string? ToIsoDate(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;

        raw = raw.Trim();
        var formats = new[]
        {
        "d/M/yyyy","dd/M/yyyy","d/MM/yyyy","dd/MM/yyyy",
        "d-M-yyyy","dd-M-yyyy","d-MM-yyyy","dd-MM-yyyy"
    };

        if (DateTime.TryParseExact(raw, formats, CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var dt))
            return dt.ToString("yyyy-MM-dd");

        return null;
    }


    // ----------------- helpers -----------------

  



    private static string? FindByRegex(string text, string pattern)
        {
            var m = Regex.Match(text, pattern, RegexOptions.IgnoreCase | RegexOptions.Multiline);
            if (!m.Success) return null;
            return m.Groups.Count > 1 ? m.Groups[1].Value.Trim() : m.Value.Trim();
        }

      

        private static int CountWords(string s)
            => Regex.Matches(s ?? "", @"\b\w+\b").Count;

        private static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }
}
