using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ProductionPlanController : ControllerBase
    {
        private readonly IProductionPlanRepository _repo;
        public ProductionPlanController(IProductionPlanRepository repo) => _repo = repo;

        [HttpGet("salesorders")]
        public async Task<IActionResult> GetSalesOrders([FromQuery] int? includeSoId)
     => Ok(await _repo.GetSalesOrdersAsync(includeSoId));

        // GET: api/production-planning/so/5?warehouseId=1
        [HttpGet("so/{salesOrderId:int}")]
        public async Task<IActionResult> GetBySo(int salesOrderId, [FromQuery] int warehouseId)
            => Ok(await _repo.GetBySalesOrderAsync(salesOrderId, warehouseId));

        // POST: api/production-planning/save
        [HttpPost("save")]
        public async Task<IActionResult> Save([FromBody] SavePlanRequest req)
        {
            var id = await _repo.SavePlanAsync(req);
            return Ok(new { productionPlanId = id });
        }
        [HttpGet("list-with-lines")]
        public async Task<IActionResult> ListWithLines()
        {
            var data = await _repo.ListPlansWithLinesAsync();
            return Ok(new { isSuccess = true, data });
        }

        [HttpGet("shortage-grn-alerts")]
        public async Task<IActionResult> GetShortageGrnAlerts()
        {
            var list = await _repo.GetShortageGrnAlertsAsync();
            return Ok(new { data = list, count = list.Count() });
        }
        [HttpPut("update")]
        public async Task<IActionResult> Update([FromBody] ProductionPlanUpdateRequest req)
        {
            if (req == null || req.Id <= 0)
                return BadRequest(new { message = "Invalid plan id" });

            if (req.Lines == null || req.Lines.Count == 0)
                return BadRequest(new { message = "Lines required" });

            var id = await _repo.UpdateAsync(req);
            return Ok(new { isSuccess = true, productionPlanId = id });
        }

        // DELETE: api/ProductionPlan/123
        [HttpDelete("{id:int}")]
        public async Task<IActionResult> Delete(int id)
        {
            var pid = await _repo.DeleteAsync(id);
            return Ok(new { isSuccess = true, productionPlanId = pid });
        }
        // GET api/ProductionPlan/123
        [HttpGet("{id:int}")]
        public async Task<IActionResult> GetById(int id)
        {
            var dto = await _repo.GetByIdAsync(id);
            return Ok(new { isSuccess = true, data = dto });
        }

    }
}
