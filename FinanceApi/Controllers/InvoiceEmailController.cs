using System.Threading.Tasks;
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class InvoiceEmailController : ControllerBase
    {
        private readonly InterfaceService.IEmailService _emailService;
        private readonly IPdfService _pdfService;
        private readonly IEmailRepository _emailRepo;

        public InvoiceEmailController(
            InterfaceService.IEmailService emailService,
            IPdfService pdfService,
            IEmailRepository emailRepo)
        {
            _emailService = emailService;
            _pdfService = pdfService;
            _emailRepo = emailRepo;
        }

        // ============================
        // 1) INVOICE DROPDOWN
        // ============================
        // GET: api/invoiceemail/invoices?docType=SI
        [HttpGet("invoices")]
        public async Task<IActionResult> GetInvoices([FromQuery] string docType)
        {
            if (string.IsNullOrWhiteSpace(docType))
                return BadRequest("docType is required (SI or PIN).");

            docType = docType.ToUpperInvariant();
            if (docType != "SI" && docType != "PIN")
                return BadRequest("Invalid docType. Use 'SI' or 'PIN'.");

            var list = await _emailRepo.GetInvoiceListAsync(docType);
            return Ok(list);
        }

        // ============================
        // 2) EMAIL TEMPLATE
        // ============================
        // GET: api/invoiceemail/template/1?docType=SI
        [HttpGet("template/{id:int}")]
        public async Task<IActionResult> GetTemplate(int id, [FromQuery] string? docType = null)
        {
            if (!string.IsNullOrWhiteSpace(docType))
                docType = docType.ToUpperInvariant();

            var tmpl = await _emailRepo.GetTemplateAsync(id, docType);
            if (tmpl == null)
                return NotFound();

            return Ok(tmpl);
        }

        [HttpPost("sales/{invoiceId:int}")]
        public async Task<ActionResult<EmailResultDto>> SendSalesInvoice(
    int invoiceId,
    [FromBody] EmailRequestDto dto)
        {
            if (!ModelState.IsValid)
                return BadRequest(new EmailResultDto
                {
                    Success = false,
                    Message = "Invalid email request"
                });

            if (string.IsNullOrWhiteSpace(dto.ToEmail))
                return BadRequest(new EmailResultDto
                {
                    Success = false,
                    Message = "ToEmail is required."
                });

            // ✅ Use route id
            var pdfBytes = await _pdfService.GenerateSalesInvoicePdfAsync(invoiceId);

            var result = await _emailService.SendInvoiceEmailAsync(dto, pdfBytes);
            if (!result.Success)
                return StatusCode(500, result);

            return Ok(result);
        }

        [HttpPost("pin/{invoiceId:int}")]
        public async Task<ActionResult<EmailResultDto>> SendSupplierInvoice(
            int invoiceId,
            [FromBody] EmailRequestDto dto)
        {
            if (!ModelState.IsValid)
                return BadRequest(new EmailResultDto
                {
                    Success = false,
                    Message = "Invalid email request"
                });

            if (string.IsNullOrWhiteSpace(dto.ToEmail))
                return BadRequest(new EmailResultDto
                {
                    Success = false,
                    Message = "ToEmail is required."
                });

            // ✅ Use route id
            var pdfBytes = await _pdfService.GenerateSupplierInvoicePdfAsync(invoiceId);

            var result = await _emailService.SendInvoiceEmailAsync(dto, pdfBytes);
            if (!result.Success)
                return StatusCode(500, result);

            return Ok(result);
        }

        [HttpGet("invoiceinfo/{docType}/{invoiceId:int}")]
        public async Task<IActionResult> GetInvoiceInfo(string docType, int invoiceId)
        {
            if (invoiceId <= 0)
                return BadRequest("Invalid invoice id.");

            docType = docType.ToUpperInvariant();
            if (docType != "SI" && docType != "PIN")
                return BadRequest("Invalid docType. Use 'SI' or 'PIN'.");

            var info = await _emailRepo.GetInvoiceInfoAsync(docType, invoiceId);
            if (info == null)
                return NotFound("Invoice not found.");

            return Ok(info);
        }
    }
}
