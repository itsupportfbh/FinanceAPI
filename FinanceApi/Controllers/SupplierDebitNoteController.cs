// Controllers/SupplierDebitNoteController.cs
using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SupplierDebitNoteController : ControllerBase
    {
        private readonly ISupplierDebitNoteService _service;

        public SupplierDebitNoteController(ISupplierDebitNoteService service)
        {
            _service = service;
        }

        [HttpGet("GetAll")]
        public async Task<IActionResult> GetAll()
            => Ok(new ResponseResult(true, "Retrieved", await _service.GetAllAsync()));

        [HttpGet("GetById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var data = await _service.GetByIdAsync(id);
            return Ok(new ResponseResult(data != null, data != null ? "Success" : "Not found", data));
        }

        [HttpPost("Create")]
        public async Task<IActionResult> Create([FromBody] SupplierDebitNote dto)
        {
            var id = await _service.CreateAsync(dto);
            return Ok(new ResponseResult(true, "Debit Note Created", id));
        }

        [HttpPut("Update/{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] SupplierDebitNote dto)
        {
            dto.Id = id;
            await _service.UpdateAsync(dto);
            return Ok(new ResponseResult(true, "Debit Note Updated", null));
        }

        [HttpDelete("Delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteAsync(id);
            return Ok(new ResponseResult(true, "Debit Note Deleted", null));
        }
        // Controllers/SupplierInvoicePinController.cs
        [HttpGet("GetDebitNoteSource/{id}")]
        public async Task<IActionResult> GetDebitNoteSource(int id)
        {
            var data = await _service.GetDebitNoteSourceAsync(id);
            return Ok(new ResponseResult(data != null, data != null ? "Success" : "Not found", data));
        }
        [HttpPost("MarkDebitNote/{id}")]
        public async Task<IActionResult> MarkDebitNote(int id)
        {
            var userName = User?.Identity?.Name ?? "system";
            await _service.MarkDebitNoteAsync(id, userName);

            return Ok(new ResponseResult(true, "Supplier Invoice marked as Debit Note created", null));
        }
    }
}
