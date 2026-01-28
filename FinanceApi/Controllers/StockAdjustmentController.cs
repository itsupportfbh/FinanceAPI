using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StockAdjustmentController : ControllerBase
    {
        private readonly IStockAdjustmentServices _service;
        public StockAdjustmentController(IStockAdjustmentServices service)
        {
            _service = service;
        }
        [HttpGet("GetBinDetailsbywarehouseID/{id}")]
        public async Task<IActionResult> GetBinDetailsbywarehouseID(int id)
        {
            var licenseObj = await _service.GetBinDetailsbywarehouseID(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpGet("GetItemDetailswithwarehouseandBinID/{warehouseId}/{binId}")]
        public async Task<IActionResult> GetItemDetailswithwarehouseandBinID(int warehouseId, int binId)
        {
            var licenseObj = await _service.GetItemDetailswithwarehouseandBinID(warehouseId, binId);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }
    }
}
