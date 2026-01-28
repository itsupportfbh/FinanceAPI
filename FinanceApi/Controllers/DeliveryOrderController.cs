using FinanceApi.InterfaceService;
using FinanceApi.Interfaces;
using FinanceApi.Repositories;
using Microsoft.AspNetCore.Mvc;
using static FinanceApi.ModelDTO.DeliveryOrderDtos;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DeliveryOrderController : ControllerBase
    {
        private readonly IDeliveryOrderService _svc;
        public DeliveryOrderController(IDeliveryOrderService svc) => _svc = svc;

        private IActionResult OkData(object? data, string msg = "Success", bool ok = true)
            => Ok(new { isSuccess = ok, message = msg, data });

        [HttpPost("Create")]
        public async Task<IActionResult> Create([FromBody] DoCreateRequest req)
        {
            var id = await _svc.CreateAsync(req, userId: 1);
            return OkData(id, "Created");
        }

        [HttpGet("GetAll")]
        public async Task<IActionResult> GetAll()
        {
            var rows = await _svc.GetAllAsync();
            return OkData(rows);
        }

        [HttpGet("GetById/{id:int}")]
        public async Task<IActionResult> GetById(int id)
        {
            var (hdr, lines) = await _svc.GetAsync(id);
            if (hdr is null) return NotFound(new { isSuccess = false, message = "Not found" });
            return OkData(new { header = hdr, lines });
        }

        [HttpGet("GetLines/{id:int}")]
        public async Task<IActionResult> GetLines(int id)
        {
            var lines = await _svc.GetLinesAsync(id);
            return OkData(lines);
        }

        [HttpPut("Update/{id:int}/Header")]
        public async Task<IActionResult> UpdateHeader(int id, [FromBody] DoUpdateHeaderRequest req)
        {
            await _svc.UpdateHeaderAsync(id, req, userId: 1);
            return OkData(true, "Updated");
        }

        [HttpPost("AddLine")]
        public async Task<IActionResult> AddLine([FromBody] DoAddLineRequest req)
        {
            var lineId = await _svc.AddLineAsync(req, userId: 1);
            return OkData(lineId, "Line added");
        }

        [HttpDelete("RemoveLine/{lineId:int}")]
        public async Task<IActionResult> RemoveLine(int lineId)
        {
            await _svc.RemoveLineAsync(lineId);
            return OkData(true, "Removed");
        }

        //[HttpPost("Submit/{id:int}")]
        //public async Task<IActionResult> Submit(int id) { await _svc.submit(id, 1); return OkData(true); } // optional

        [HttpPost("Approve/{id:int}")]
        public async Task<IActionResult> Approve(int id) { await _svc.ApproveAsync(id, 1); return OkData(true); }

        [HttpPost("Reject/{id:int}")]
        public async Task<IActionResult> Reject(int id) { await _svc.RejectAsync(id, 1); return OkData(true); }

        [HttpPost("Post/{id:int}")]
        public async Task<IActionResult> Post(int id) { await _svc.PostAsync(id, 1); return OkData(true, "Posted"); }

        // Redelivery snapshot for the edit view
        [HttpGet("SoSnapshot/{id:int}")]
        public async Task<IActionResult> GetSoSnapshot(int id, [FromServices] IDeliveryOrderRepository repo)
        {
            var hdr = await repo.GetHeaderAsync(id);
            if (hdr == null || hdr.SoId is null)
                return Ok(new { isSuccess = true, message = "OK", data = Array.Empty<object>() });

            var rows = await (repo as DeliveryOrderRepository)!.GetSoRedeliveryViewAsync(id, hdr.SoId.Value);
            return Ok(new { isSuccess = true, message = "OK", data = rows });
        }
    }
}
