using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading.Tasks;

[Route("api/[controller]")]
[ApiController]
public class ApAgingController : ControllerBase
{
    private readonly IApAgingService _service;

    public ApAgingController(IApAgingService service)
    {
        _service = service;
    }

    [HttpGet("summary")]
    public async Task<IActionResult> GetSummary(DateTime fromDate, DateTime toDate)
    {
        var data = await _service.GetSummaryAsync(fromDate, toDate);
        return Ok(new { isSuccess = true, data });
    }

    [HttpGet("supplierInvoices")]
    public async Task<IActionResult> GetSupplierInvoices(
        int supplierId,
        DateTime fromDate,
        DateTime toDate)
    {
        var data = await _service.GetSupplierInvoicesAsync(supplierId, fromDate, toDate);
        return Ok(new { isSuccess = true, data });
    }
}
