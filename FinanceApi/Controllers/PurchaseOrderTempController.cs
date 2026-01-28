using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class PurchaseOrderTempController : ControllerBase
    {
        private readonly IPurchaseOrderTempService _svc;

        public PurchaseOrderTempController(IPurchaseOrderTempService svc)
        {
            _svc = svc;
        }

        [HttpGet("getAll")]
        public async Task<IActionResult> GetDrafts([FromQuery] string? createdBy = null)
        {
            var drafts = await _svc.GetDraftsAsync(createdBy);
            return Ok(new ResponseResult(true, "Drafts fetched", drafts));
        }

        [HttpGet("get/{id:int}")]
        public async Task<IActionResult> GetDraft(int id)
        {
            var draft = await _svc.GetByIdAsync(id);
            if (draft is null) return NotFound(new ResponseResult(false, "Draft not found", null));
            return Ok(new ResponseResult(true, "Draft fetched", draft));
        }

        [HttpPost("insert")]
        public async Task<IActionResult> CreateDraft([FromBody] PurchaseOrderTemp req)
        {
            var id = await _svc.CreateAsync(req);
            return Ok(new ResponseResult(true, "Draft created", new { id }));
        }

        [HttpPut("update/{id:int}")]
        public async Task<IActionResult> UpdateDraft(int id, [FromBody] PurchaseOrderTemp req)
        {
            var exists = await _svc.GetByIdAsync(id);
            if (exists is null) return NotFound(new ResponseResult(false, "Draft not found", null));
            req.Id = id;
            await _svc.UpdateAsync(req);
            return Ok(new ResponseResult(true, "Draft updated", null));
        }


        [HttpDelete("delete/{id:int}")]
        public async Task<IActionResult> DeleteDraft(int id)
        {
            var exists = await _svc.GetByIdAsync(id);
            if (exists is null) return NotFound(new ResponseResult(false, "Draft not found", null));
            await _svc.DeleteAsync(id);
            return Ok(new ResponseResult(true, "Draft deleted", null));
        }

        [HttpPost("promote/{id:int}")]
        public async Task<IActionResult> Promote(int id, [FromBody] string userId)
        {
            var exists = await _svc.GetByIdAsync(id);
            if (exists is null) return NotFound(new ResponseResult(false, "Draft not found", null));
            var poId = await _svc.PromoteAsync(id, userId ?? "system");
            return Ok(new ResponseResult(true, "Draft promoted to PO", new { id = poId }));
        }
    }
}
