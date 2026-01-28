// FinanceApi/Services/PdfService.cs
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Dapper;
using FinanceApi.Data;
using FinanceApi.InterfaceService;
using Microsoft.Extensions.Logging;
using QuestPDF.Fluent;
using QuestPDF.Helpers;
using QuestPDF.Infrastructure;

namespace FinanceApi.Services
{
    public class PdfService : IPdfService
    {
        private readonly IDbConnectionFactory _factory;
        private readonly ILogger<PdfService> _logger;

        public PdfService(IDbConnectionFactory factory, ILogger<PdfService> logger)
        {
            _factory = factory;
            _logger = logger;

            QuestPDF.Settings.License = LicenseType.Community;
        }

        #region ===== PUBLIC METHODS (called from controller) =====

        // ---------- SALES INVOICE (SI) ----------
        public async Task<byte[]> GenerateSalesInvoicePdfAsync(int salesInvoiceId)
        {
            var header = await GetSalesInvoiceHeaderAsync(salesInvoiceId);
            if (header == null)
                throw new Exception($"SalesInvoice Id {salesInvoiceId} not found.");

            var lines = (await GetSalesInvoiceLinesAsync(salesInvoiceId)).ToList();

            // GST = Total - Subtotal - Shipping
            var subtotal = header.Subtotal;
            var shipping = header.ShippingCost;
            var grandTotal = header.Total;
            var gstAmount = grandTotal - subtotal - shipping;

            var pdfBytes = Document.Create(container =>
            {
                container.Page(page =>
                {
                    page.Margin(40);
                    page.Size(PageSizes.A4);
                    page.DefaultTextStyle(x => x.FontSize(11));

                    // ----- HEADER -----
                    page.Header().Stack(stack =>
                    {
                        stack.Spacing(4);
                        stack.Item().Text("FB HOLDINGS PTE LTD")
                            .FontSize(18).SemiBold();

                        stack.Item().Text($"Invoice #{header.InvoiceNo}")
                            .FontSize(16).SemiBold();

                        stack.Item().Text($"Date: {header.InvoiceDate:dd-MMM-yyyy}");
                    });

                    // ----- CONTENT -----
                    page.Content().PaddingTop(15).Stack(content =>
                    {
                        // Lines table
                        content.Item().Element(e => BuildSalesLinesTable(e, lines));

                        // Totals
                        content.Item().PaddingTop(200).AlignRight().Stack(totals =>
                        {
                            totals.Spacing(2);
                            totals.Item().Text($"Sub Total: {subtotal:0.00}");
                            totals.Item().Text($"GST Amount: {gstAmount:0.00}");
                            
                            totals.Item().Text($"Grand Total: {grandTotal:0.00}")
                                  .FontSize(13).Bold();
                        });
                    });

                    // ----- FOOTER -----
                    page.Footer().AlignCenter().Text(x =>
                    {
                        x.Span("Thank you for your business.").FontSize(10);
                    });
                });
            }).GeneratePdf();

            return pdfBytes;
        }

        // ---------- SUPPLIER INVOICE (PIN) ----------
        public async Task<byte[]> GenerateSupplierInvoicePdfAsync(int supplierInvoiceId)
        {
            var header = await GetSupplierInvoiceHeaderAsync(supplierInvoiceId);
            if (header == null)
                throw new Exception($"SupplierInvoicePin Id {supplierInvoiceId} not found.");

            var lines = (await GetSupplierInvoiceLinesAsync(supplierInvoiceId)).ToList();

            // Subtotal = sum of line totals (before GST)
            var subtotal = lines.Sum(l => l.LineTotal);

            // GST amount = Amount (gross) - subtotal
            var gstAmount = header.Amount - subtotal;

            var pdfBytes = Document.Create(container =>
            {
                container.Page(page =>
                {
                    page.Margin(40);
                    page.Size(PageSizes.A4);
                    page.DefaultTextStyle(x => x.FontSize(11));

                    // ----- HEADER -----
                    page.Header().Stack(stack =>
                    {
                        stack.Spacing(4);
                        stack.Item().Text("FB HOLDINGS PTE LTD")
                            .FontSize(18).SemiBold();

                        stack.Item().Text($"Supplier Invoice #{header.InvoiceNo}")
                            .FontSize(16).SemiBold();

                        stack.Item().Text($"Date: {header.InvoiceDate:dd-MMM-yyyy}");

                        if (!string.IsNullOrWhiteSpace(header.SupplierName))
                        {
                            stack.Item().Text($"Supplier: {header.SupplierName}");
                        }
                    });

                    // ----- CONTENT -----
                    page.Content().PaddingTop(15).Stack(content =>
                    {
                        // Lines table
                        content.Item().Element(e => BuildSupplierLinesTable(e, lines));

                        // Totals
                        content.Item().PaddingTop(200).AlignRight().Stack(totals =>
                        {
                            totals.Spacing(2);
                            totals.Item().Text($"Sub Total (Before GST): {subtotal:0.00}");
                            totals.Item().Text($"GST ({header.GstPct:0.##}%): {gstAmount:0.00}");
                            totals.Item().Text($"Grand Total: {header.Amount:0.00}")
                                  .FontSize(13).Bold();
                        });
                    });

                    // ----- FOOTER -----
                    page.Footer().AlignCenter().Text(x =>
                    {
                        x.Span("Thank you.").FontSize(10);
                    });
                });
            }).GeneratePdf();

            return pdfBytes;
        }

        #endregion

        #region ===== SALES INVOICE DB =====

        private async Task<SalesInvoiceHeaderDto?> GetSalesInvoiceHeaderAsync(int invoiceId)
        {
            const string sql = @"
SELECT 
    si.Id,
    si.InvoiceNo,
    si.InvoiceDate,
    si.Subtotal,
    si.ShippingCost,
    si.Total
FROM dbo.SalesInvoice si
WHERE si.Id = @Id
  AND si.IsActive = 1;";

            using var conn = _factory.CreateConnection();
            return await conn.QuerySingleOrDefaultAsync<SalesInvoiceHeaderDto>(sql, new { Id = invoiceId });
        }

        private async Task<IEnumerable<SalesInvoiceLineDto>> GetSalesInvoiceLinesAsync(int invoiceId)
        {
            const string sql = @"
SELECT 
    ItemName,
    Uom,
    Qty,
    UnitPrice,
    DiscountPct,
    GstPct,
    Tax,
    LineAmount
FROM dbo.SalesInvoiceLine
WHERE SiId = @Id;";

            using var conn = _factory.CreateConnection();
            return await conn.QueryAsync<SalesInvoiceLineDto>(sql, new { Id = invoiceId });
        }

        #endregion

        #region ===== SUPPLIER INVOICE DB (SupplierInvoicePin) =====

        private async Task<SupplierInvoiceHeaderDto?> GetSupplierInvoiceHeaderAsync(int pinId)
        {
            const string sql = @"
SELECT 
    pin.Id,
    pin.InvoiceNo,
    pin.InvoiceDate,
    pin.Amount,
    pin.Tax      AS GstPct,
    pin.SupplierId,
    ISNULL(s.Name, '') AS SupplierName
FROM dbo.SupplierInvoicePin pin
LEFT JOIN dbo.Supplier s
       ON s.Id = pin.SupplierId
WHERE pin.Id = @Id
  AND pin.IsActive = 1;";

            using var conn = _factory.CreateConnection();
            return await conn.QuerySingleOrDefaultAsync<SupplierInvoiceHeaderDto>(sql, new { Id = pinId });
        }

        private async Task<IEnumerable<SupplierInvoiceLineDto>> GetSupplierInvoiceLinesAsync(int pinId)
        {
            // We just need LinesJson + Tax from SupplierInvoicePin
            const string sql = @"
SELECT 
    LinesJson,
    Tax       AS GstPct,
    Amount
FROM dbo.SupplierInvoicePin
WHERE Id = @Id
  AND IsActive = 1;";

            using var conn = _factory.CreateConnection();
            var raw = await conn.QuerySingleOrDefaultAsync<SupplierInvoiceRawDto>(sql, new { Id = pinId });

            if (raw == null || string.IsNullOrWhiteSpace(raw.LinesJson))
                return Enumerable.Empty<SupplierInvoiceLineDto>();

            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };

            // JSON: [{"item":"AC","location":"Nungampakkam","qty":900,"unitPrice":100,"discountPct":2,"lineTotal":88200,...}]
            var jsonLines = JsonSerializer.Deserialize<List<SupplierInvoiceJsonLine>>(raw.LinesJson, options)
                            ?? new List<SupplierInvoiceJsonLine>();

            var list = new List<SupplierInvoiceLineDto>();

            foreach (var j in jsonLines)
            {
                var dto = new SupplierInvoiceLineDto
                {
                    ItemName = j.Item ?? string.Empty,
                    Location = j.Location ?? string.Empty,
                    Qty = j.Qty,
                    UnitPrice = j.UnitPrice,
                    DiscountPct = j.DiscountPct,
                    LineTotal = j.LineTotal,
                    GstPct = raw.GstPct
                };

                list.Add(dto);
            }

            return list;
        }

        #endregion

        #region ===== TABLE BUILDERS =====

        private static void BuildSalesLinesTable(IContainer container, IList<SalesInvoiceLineDto> lines)
        {
            container.Table(table =>
            {
                table.ColumnsDefinition(columns =>
                {
                    columns.RelativeColumn(4); // Item
                    columns.RelativeColumn(1); // UOM
                    columns.RelativeColumn(1); // Qty
                    columns.RelativeColumn(2); // Unit Price
                    columns.RelativeColumn(1); // Disc %
                    columns.RelativeColumn(1); // GST %
                    columns.RelativeColumn(2); // Tax (text)
                    columns.RelativeColumn(2); // Amount
                });

                table.Header(header =>
                {
                    header.Cell().Element(HeaderCell).Text("Item");
                    header.Cell().Element(HeaderCell).Text("UOM");
                    header.Cell().Element(HeaderCell).AlignRight().Text("Qty");
                    header.Cell().Element(HeaderCell).AlignRight().Text("Unit Price");
                    header.Cell().Element(HeaderCell).AlignRight().Text("Disc %");
                    header.Cell().Element(HeaderCell).AlignRight().Text("GST %");
                    header.Cell().Element(HeaderCell).AlignRight().Text("Tax");
                    header.Cell().Element(HeaderCell).AlignRight().Text("Amount");
                });

                foreach (var l in lines)
                {
                    table.Cell().Element(BodyCell).Text(l.ItemName);
                    table.Cell().Element(BodyCell).Text(l.Uom);
                    table.Cell().Element(BodyCell).AlignRight().Text(l.Qty.ToString("0.##"));
                    table.Cell().Element(BodyCell).AlignRight().Text(l.UnitPrice.ToString("0.00"));
                    table.Cell().Element(BodyCell).AlignRight()
                         .Text(l.DiscountPct.HasValue ? l.DiscountPct.Value.ToString("0.##") : "");
                    table.Cell().Element(BodyCell).AlignRight()
                         .Text(l.GstPct.HasValue ? l.GstPct.Value.ToString("0.##") : "");
                    table.Cell().Element(BodyCell).AlignRight().Text(l.Tax ?? "");
                    table.Cell().Element(BodyCell).AlignRight().Text(l.LineAmount.ToString("0.00"));
                }

                static IContainer HeaderCell(IContainer c) =>
                    c.BorderBottom(1)
                     .BorderColor(Colors.Grey.Lighten2)
                     .DefaultTextStyle(x => x.SemiBold())
                     .PaddingVertical(3);

                static IContainer BodyCell(IContainer c) =>
                    c.PaddingVertical(2);
            });
        }

        private static void BuildSupplierLinesTable(IContainer container, IList<SupplierInvoiceLineDto> lines)
        {
            container.Table(table =>
            {
                table.ColumnsDefinition(columns =>
                {
                    columns.RelativeColumn(4); // Item
                    columns.RelativeColumn(3); // Location
                    columns.RelativeColumn(1); // Qty
                    columns.RelativeColumn(2); // Unit Price
                    columns.RelativeColumn(1); // Disc %
                    columns.RelativeColumn(2); // Line Total
                });

                table.Header(header =>
                {
                    header.Cell().Element(HeaderCell).Text("Item");
                    header.Cell().Element(HeaderCell).Text("Location");
                    header.Cell().Element(HeaderCell).AlignRight().Text("Qty");
                    header.Cell().Element(HeaderCell).AlignRight().Text("Unit Price");
                    header.Cell().Element(HeaderCell).AlignRight().Text("Disc %");
                    header.Cell().Element(HeaderCell).AlignRight().Text("Amount");
                });

                foreach (var l in lines)
                {
                    table.Cell().Element(BodyCell).Text(l.ItemName);
                    table.Cell().Element(BodyCell).Text(l.Location);
                    table.Cell().Element(BodyCell).AlignRight().Text(l.Qty.ToString("0.##"));
                    table.Cell().Element(BodyCell).AlignRight().Text(l.UnitPrice.ToString("0.00"));
                    table.Cell().Element(BodyCell).AlignRight().Text(l.DiscountPct.ToString("0.##"));
                    table.Cell().Element(BodyCell).AlignRight().Text(l.LineTotal.ToString("0.00"));
                }

                static IContainer HeaderCell(IContainer c) =>
                    c.BorderBottom(1)
                     .BorderColor(Colors.Grey.Lighten2)
                     .DefaultTextStyle(x => x.SemiBold())
                     .PaddingVertical(3);

                static IContainer BodyCell(IContainer c) =>
                    c.PaddingVertical(2);
            });
        }

        #endregion

        #region ===== PRIVATE DTOs =====

        // --- Sales Invoice DTOs ---
        private class SalesInvoiceHeaderDto
        {
            public int Id { get; set; }
            public string InvoiceNo { get; set; } = string.Empty;
            public DateTime InvoiceDate { get; set; }
            public decimal Subtotal { get; set; }
            public decimal ShippingCost { get; set; }
            public decimal Total { get; set; }
        }

        private class SalesInvoiceLineDto
        {
            public string ItemName { get; set; } = string.Empty;
            public string Uom { get; set; } = string.Empty;
            public decimal Qty { get; set; }
            public decimal UnitPrice { get; set; }
            public decimal? DiscountPct { get; set; }
            public decimal? GstPct { get; set; }
            public string? Tax { get; set; }   // EXCLUSIVE / INCLUSIVE
            public decimal LineAmount { get; set; }   // Amount shown in PDF
        }

        // --- Supplier Invoice DTOs ---
        private class SupplierInvoiceHeaderDto
        {
            public int Id { get; set; }
            public string InvoiceNo { get; set; } = string.Empty;
            public DateTime InvoiceDate { get; set; }
            public decimal Amount { get; set; }   // Gross total (with GST)
            public decimal GstPct { get; set; }   // Tax column in table
            public int SupplierId { get; set; }
            public string SupplierName { get; set; } = string.Empty;
        }

        private class SupplierInvoiceRawDto
        {
            public string LinesJson { get; set; } = string.Empty;
            public decimal GstPct { get; set; }  // Tax column
            public decimal Amount { get; set; }
        }

        // JSON structure in LinesJson
        private class SupplierInvoiceJsonLine
        {
            public string Item { get; set; } = string.Empty;
            public string Location { get; set; } = string.Empty;
            public decimal Qty { get; set; }
            public decimal UnitPrice { get; set; }
            public decimal DiscountPct { get; set; }
            public decimal LineTotal { get; set; }
            // dcNoteNo, matchStatus, mismatchFields... (ignored)
        }

        private class SupplierInvoiceLineDto
        {
            public string ItemName { get; set; } = string.Empty;
            public string Location { get; set; } = string.Empty;
            public decimal Qty { get; set; }
            public decimal UnitPrice { get; set; }
            public decimal DiscountPct { get; set; }
            public decimal LineTotal { get; set; }    // before GST
            public decimal GstPct { get; set; }
        }

        #endregion
    }
}
