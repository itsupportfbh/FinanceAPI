using FinanceApi.InterfaceService;
using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("api/mobile-receiving")]
public class MobileReceivingController : ControllerBase
{
    private readonly IMobileReceivingService _svc;
    private void EnsureTokenOk(string poNo)
    {
        var t = Request.Headers["X-MR-TOKEN"].ToString();
        var tokenSvc = HttpContext.RequestServices.GetRequiredService<IMobileLinkTokenService>();

        if (!tokenSvc.TryValidate(t, poNo, out var err))
            throw new Exception("Access denied: " + err);
    }
    public MobileReceivingController(IMobileReceivingService svc) => _svc = svc;

    [HttpGet("po")]
    public async Task<ActionResult<PoVm>> GetPo([FromQuery] string poNo)
    {
        try
        {
            EnsureTokenOk(poNo);
            return Ok(await _svc.GetPurchaseOrderAsync(poNo));
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }

    [HttpPost("scan")]
    public async Task<IActionResult> Scan([FromBody] ScanReq req)
    {
        try
        {
            EnsureTokenOk(req.PurchaseOrderNo);
            await _svc.ValidateScanAsync(req);
            return Ok(new { message = "OK" });
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }

    [HttpPost("sync")]
    public async Task<IActionResult> Sync([FromBody] SyncReq req)
    {
        try
        {
            EnsureTokenOk(req.PurchaseOrderNo);
            await _svc.SyncAsync(req);
            return Ok(new { message = "Synced" });
        }
        catch (Exception ex)
        {
            return BadRequest(new { message = ex.Message });
        }
    }
}
