using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.ModelDTO.TB;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class FinanceReportController : ControllerBase
    {
        private readonly IFinanceReportService _service;

        public FinanceReportController(IFinanceReportService service)
        {
            _service = service;
        }

        [HttpPost("trial-balance")]
        public async Task<IActionResult> TrialBalance([FromBody] ReportBaseDTO dto)
        {
            var data = await _service.GetTrialBalanceAsync(dto);
            var result = new ResponseResult(true, "Success", data);
            return Ok(result);
        }

        // 🔹 new: TB account detail
        [HttpPost("trial-balance-detail")]
        public async Task<IActionResult> TrialBalanceDetail(
            [FromBody] TrialBalanceDetailRequestDTO dto)
        {
            var data = await _service.GetTrialBalanceDetailAsync(dto);
            return Ok(new { data });
        }


        [HttpGet("GetProfitLossDetails")]
        public async Task<IActionResult> GetProfitLossDetails()
        {
            var list = await _service.GetProfitLossDetails();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }

        [HttpGet("GetBalanceSheetDetails")]
        public async Task<IActionResult> GetBalanceSheetDetails()
        {
            var list = await _service.GetBalanceSheetAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }

        [HttpPost("daybook")]
        public async Task<IActionResult> GetDaybook([FromBody] ReportBaseDTO dto)
        {
            // dto.FromDate, dto.ToDate will come from frontend
            var list = await _service.GetDaybookAsync(dto);
            var result = new ResponseResult(true, "Success", list);
            return Ok(result);
        }

        [HttpPost("trial-balance/opening-balance")]
        public async Task<IActionResult> SaveOpeningBalance([FromBody] OpeningBalanceEditDto dto)
        {
            await _service.SaveOpeningBalanceAsync(dto, dto.UserName);
            return Ok(new { success = true });
        }
    }
}
