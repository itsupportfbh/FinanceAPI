using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace UnityWorksERP.Finance.AR
{
    [ApiController]
    [Route("api/[controller]")]
    public class ArInvoiceController : ControllerBase
    {
        private readonly IArInvoiceService _service;

        public ArInvoiceController(IArInvoiceService service)
        {
            _service = service;
        }

        // GET api/ArInvoice/list
        [HttpGet("list")]
        public async Task<IActionResult> GetList()
        {
            var data = await _service.GetAllAsync();
            return Ok(new { data });
        }
        [HttpPost("advance")]
        public async Task<IActionResult> CreateAdvance([FromBody] ArAdvanceDto dto)
        {
            if (dto.CustomerId <= 0 || dto.Amount <= 0)
                return BadRequest("Customer and positive Amount are required.");

            if (dto.AdvanceDate == default)
                dto.AdvanceDate = DateTime.Today;

            var id = await _service.CreateAdvanceAsync(dto);
            return Ok(new { advanceId = id });
        }

        // GET: api/ar/advance/open?customerId=1&salesOrderId=10
        [HttpGet("advance/open")]
        public async Task<IActionResult> GetOpenAdvances([FromQuery] int customerId, [FromQuery] int? salesOrderId)
        {
            if (customerId <= 0)
                return BadRequest("CustomerId required.");

            var list = await _service.GetOpenAdvancesAsync(customerId, salesOrderId);
            return Ok(list);
        }
    }
}
