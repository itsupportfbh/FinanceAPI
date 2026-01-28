using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PurchaseAlertController : ControllerBase
    {
        private readonly IPurchaseAlertService _service;

        public PurchaseAlertController(IPurchaseAlertService service)
        {
            _service = service;
        }
        [HttpGet("unread")]
        public async Task<IActionResult> GetUnread()
        {
            var data = await _service.GetUnreadAsync();
            return Ok(new { isSuccess = true, message = "Success", data });
        }

        [HttpPost("{id:int}/read")]
        public async Task<IActionResult> MarkRead(int id)
        {
            await _service.MarkReadAsync(id);
            return Ok(new { isSuccess = true });
        }

        [HttpPost("read-all")]
        public async Task<IActionResult> MarkAll()
        {
            await _service.MarkAllReadAsync();
            return Ok(new { isSuccess = true });
        }

    }
}
