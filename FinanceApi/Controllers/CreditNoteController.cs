using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CreditNoteController : ControllerBase
    {
        private readonly ICreditNoteService _service;

        public CreditNoteController(ICreditNoteService service)
        {
            _service = service;
        }

        // ---------- CRUD ----------

        [HttpGet("getAll")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            return Ok(new ResponseResult(true, "Success", list));
        }

        [HttpGet("get/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var dto = await _service.GetByIdAsync(id);
            return Ok(new ResponseResult(true, "Success", dto));
        }

        [HttpPost("insert")]
        public async Task<IActionResult> Create([FromBody] CreditNote model)
        {
            var id = await _service.CreateAsync(model);
            return Ok(new ResponseResult(true, "Credit note created successfully", id));
        }

        [HttpPut("update")]
        public async Task<IActionResult> Update([FromBody] CreditNote model)
        {
            await _service.UpdateAsync(model);
            return Ok(new ResponseResult(true, "Credit note updated successfully", null));
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id, [FromQuery] int updatedBy)
        {
            await _service.DeactivateAsync(id, updatedBy);
            return Ok(new ResponseResult(true, "Credit note deleted successfully", null));
        }

        // ---------- DO → SI enriched lines (for UI pool) ----------
        // Returns DO lines with UnitPrice/DiscountPct/TaxCodeId pulled from SalesInvoiceLine
        [HttpGet("dolines/{doId}")]
        public async Task<IActionResult> GetDoLines(int doId, [FromQuery] int? excludeCnId = null)
        {
            var rows = await _service.GetDoLinesAsync(doId, excludeCnId);
            return Ok(rows);
        }
    }
}
