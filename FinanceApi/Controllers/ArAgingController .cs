using FinanceApi.Data;
using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("api/[controller]")]
public class ArAgingController : ControllerBase
{
    private readonly IArAgingService _service;

    public ArAgingController(IArAgingService service)
    {
        _service = service;
    }

    // GET api/ArAging/summary?fromDate=2025-11-01&toDate=2025-11-30
    [HttpGet("summary")]
    public async Task<IActionResult> GetSummary([FromQuery] DateTime fromDate, [FromQuery] DateTime toDate)
    {
        var data = await _service.GetSummaryAsync(fromDate, toDate);
        return Ok(new ResponseResult(true, "Success", data));
    }

    // GET api/ArAging/detail/5?fromDate=2025-11-01&toDate=2025-11-30
    [HttpGet("detail/{customerId}")]
    public async Task<IActionResult> GetCustomerDetail(
        int customerId,
        [FromQuery] DateTime fromDate,
        [FromQuery] DateTime toDate)
    {
        var data = await _service.GetCustomerInvoicesAsync(customerId, fromDate, toDate);
        return Ok(new ResponseResult(true, "Success", data));
    }
}
