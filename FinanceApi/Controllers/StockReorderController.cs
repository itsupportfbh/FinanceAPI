using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class StockReorderController : ControllerBase
    {
        private readonly IStockReorderService _service;

        public StockReorderController(IStockReorderService service)
        {
            _service = service;
        }

        [HttpGet("getAll")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }

        [HttpGet("warehouse-items")]
        public async Task<IActionResult> GetWarehouseItems(
        [FromQuery] long warehouseId)   
        {
            // basic validation
            if (warehouseId <= 0) return BadRequest(new ResponseResult(false, "warehouseId is required.", null));         


            try
            {
                var items = await _service.GetWarehouseItemsAsync(warehouseId);
                return Ok(new ResponseResult(true, "Success", items));
            }
            catch (InvalidOperationException ex)  // thrown when status 1/2
            {
                // 409 is appropriate for a domain conflict
                return Conflict(new { message = ex.Message });
            }
        }


        [HttpGet("get/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetByIdAsync(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }

        [HttpPost("insert")]
        public async Task<ActionResult> Create(StockReorder stockReorder)
        {
            var id = await _service.CreateAsync(stockReorder);
            ResponseResult data = new ResponseResult(true, "StockReorder created sucessfully", id);
            return Ok(data);
        }

        [HttpPut("update")]
        public async Task<IActionResult> Update(StockReorder stockReorder)
        {
            await _service.UpdateAsync(stockReorder);
            ResponseResult data = new ResponseResult(true, "StockReorder updated successfully.", null);
            return Ok(data);
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id, [FromQuery] int updatedBy)
        {
            await _service.DeleteLicense(id, updatedBy);
            ResponseResult data = new ResponseResult(true, "StockReorder Deleted sucessfully", null);
            return Ok(data);
        }
        [HttpGet("preview/{id}")]
        public async Task<IActionResult> GetPreview(int id)
        {
            var rows = (await _service.GetReorderPreviewAsync(id))?.ToList()
                   ?? new List<ReorderPreviewLine>();  // <- never null

            // If you want success=true even when empty:
            var resp = new ResponseResult(true, "StockReorder preview fetched successfully", rows);
            return Ok(resp);
        }

    }
}
