using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class GstReturnsController : ControllerBase
    {
        private readonly IGstF5Repository _gstRepo;

        // Replace with your actual user id resolution (claims, etc.)
        private int CurrentUserId => 1;

        public GstReturnsController(IGstF5Repository gstRepo)
        {
            _gstRepo = gstRepo;
        }

        [HttpGet("years")]
        public async Task<ActionResult<IEnumerable<GstFinancialYearOptionDto>>> GetYears()
        {
            var years = await _gstRepo.GetFinancialYearsAsync();
            return Ok(years);
        }

        [HttpGet("periods/{fyStartYear:int}")]
        public async Task<ActionResult<IEnumerable<GstPeriodOptionDto>>> GetPeriods(int fyStartYear)
        {
            var periods = await _gstRepo.GetPeriodsByYearAsync(fyStartYear);
            return Ok(periods);
        }

        [HttpGet("return/{periodId:int}")]
        public async Task<ActionResult<GstReturnDto>> GetReturn(int periodId)
        {
            var dto = await _gstRepo.GetReturnForPeriodAsync(periodId, CurrentUserId);
            return Ok(dto);
        }

        [HttpPost("apply-lock")]
        public async Task<ActionResult<GstReturnDto>> ApplyAndLock([FromBody] GstApplyLockRequest req)
        {
            var updated = await _gstRepo.ApplyAndLockAsync(req, CurrentUserId);
            return Ok(updated);
        }

        [HttpGet("adjustments/{periodId:int}")]
        public async Task<ActionResult<IEnumerable<GstAdjustmentDto>>> GetAdjustments(int periodId)
        {
            var list = await _gstRepo.GetAdjustmentsAsync(periodId);
            return Ok(list);
        }

        [HttpPost("adjustments")]
        public async Task<ActionResult<GstAdjustmentDto>> SaveAdjustment([FromBody] GstAdjustmentDto dto)
        {
            var saved = await _gstRepo.SaveAdjustmentAsync(dto, CurrentUserId);
            return Ok(saved);
        }

        [HttpDelete("adjustments/{id:int}")]
        public async Task<IActionResult> DeleteAdjustment(int id)
        {
            await _gstRepo.DeleteAdjustmentAsync(id, CurrentUserId);
            return NoContent();
        }

        [HttpGet("{periodId:int}/docs")]
        public async Task<ActionResult<IEnumerable<GstDocRowDto>>> GetDocsForPeriod(int periodId)
        {
            var docs = await _gstRepo.GetDocsByPeriodAsync(periodId);
            return Ok(docs);
        }
        [HttpGet("details")]
        public async Task<ActionResult<IEnumerable<GstDetailRowDto>>> GetGstDetails(
    [FromQuery] DateTime startDate,
    [FromQuery] DateTime endDate,
    [FromQuery] string? docType,
    [FromQuery] string? search)
        {
            var rows = await _gstRepo.GetGstDetailsAsync(startDate, endDate, docType, search);
            return Ok(rows);
        }

    }
}
