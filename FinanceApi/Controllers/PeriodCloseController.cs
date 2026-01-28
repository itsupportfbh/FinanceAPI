using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PeriodCloseController : ControllerBase
    {
        private readonly IPeriodCloseService _service;

        public PeriodCloseController(IPeriodCloseService service)
        {
            _service = service;
        }

        [HttpGet("periods")]
        public async Task<IActionResult> GetPeriods()
        {
            var list = await _service.GetPeriodsAsync();
            return Ok(list);
        }

        [HttpGet("status/{periodId:int}")]
        public async Task<IActionResult> GetStatus(int periodId)
        {
            var status = await _service.GetStatusAsync(periodId);
            if (status == null) return NotFound();
            return Ok(status);
        }

        [HttpPost("lock")]
        public async Task<IActionResult> SetLock([FromBody] SetLockRequest req)
        {
            
            int userId = 1;
            var status = await _service.SetLockAsync(req.PeriodId, req.Lock, userId);
            return Ok(status);
        }

        [HttpPost("run-fx-reval")]
        public async Task<IActionResult> RunFxReval([FromBody] FxRevalRequestDto req)
        {
            int userId = 1;
            var runId = await _service.RunFxRevalAsync(req, userId);
            return Ok(new { runId });
        }
        [HttpGet("status")]
        public async Task<IActionResult> GetStatus([FromQuery] DateTime date)
        {
            var period = await _service.GetPeriodByDateAsync(date);

            if (period == null)
            {
                return Ok(new
                {
                    isSuccess = false,
                    isLocked = false,
                    message = "No accounting period found for this date."
                });
            }

            return Ok(new
            {
                isSuccess = true,
                isLocked = period.IsLocked,
                periodName = period.PeriodName,
                startDate = period.StartDate,
                endDate = period.EndDate
            });
        }
    }

    public class SetLockRequest
    {
        public int PeriodId { get; set; }
        public bool Lock { get; set; }
    }
}
