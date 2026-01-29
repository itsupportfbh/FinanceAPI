using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("api/[controller]")]
public class BatchProductionController : ControllerBase
{
    private readonly IBatchProductionService _svc;
    public BatchProductionController(IBatchProductionService svc) => _svc = svc;

    [HttpGet("list")]
    public async Task<IActionResult> List([FromQuery] int top = 200)
        => Ok(new { isSuccess = true, data = await _svc.ListAsync(top) });

    [HttpGet("{id:int}")]
    public async Task<IActionResult> Get(int id)
        => Ok(new { isSuccess = true, data = await _svc.GetByIdAsync(id) });

    // ✅ ONE API for Create/Update + Post + Reduce stock
    [HttpPost("post")]
    public async Task<IActionResult> Post([FromBody] BatchProductionSaveRequest req)
    {
        if (req == null || req.ProductionPlanId <= 0)
            return BadRequest(new { message = "ProductionPlanId required" });

        if (req.Lines == null || req.Lines.Count == 0)
            return BadRequest(new { message = "Lines required" });

        var id = await _svc.PostAndSaveAsync(req);
        return Ok(new { isSuccess = true, batchId = id, status = "Posted" });
    }

    [HttpDelete("{id:int}")]
    public async Task<IActionResult> Delete(int id)
        => Ok(new { isSuccess = true, batchId = await _svc.DeleteAsync(id) });
}
