using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StockController : ControllerBase
    {
        private readonly IStockService _service;

        public StockController(IStockService service)
        {
            _service = service;
        }


        [HttpGet("GetAllStock")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }



        [HttpPost("createStock")]
        public async Task<ActionResult> InsertStock([FromBody] List<Stock> stocks)
        {
            if (stocks == null || !stocks.Any())
                return BadRequest(new ResponseResult(false, "No stock data received", null));

            var inserted = await _service.InsertBulkAsync(stocks);

            var result = new ResponseResult(true, $"{inserted} stock record(s) inserted successfully.", inserted);
            return Ok(result);
        }



        [HttpGet("getStockById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }

        [HttpPut("updateStockById/{id}")]
        public async Task<IActionResult> Update(Stock stock)
        {
            await _service.Update(stock);
            ResponseResult data = new ResponseResult(true, "Stock updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteStockById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.Delete(id);
            ResponseResult data = new ResponseResult(true, "Stock Deleted sucessfully", null);
            return Ok(data);
        }


        [HttpGet("GetAllStockList")]
        public async Task<IActionResult> GetAllStockList()
        {
            var list = await _service.GetAllStockList();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }


        [HttpGet("GetAllItemStockList")]
        public async Task<IActionResult> GetAllItemStockList()
        {
            var list = await _service.GetAllItemStockList();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }

        [HttpPost("markAsTransferredBulk")]
        public async Task<IActionResult> MarkAsTransferredBulk([FromBody] List<MarkAsTransferredRequest> requests)
        {
            if (requests == null || requests.Count == 0)
            {
                return BadRequest(new ResponseResult(false, "No transfer requests received.", null));
            }

            // Optional: basic validation check
            if (requests.Any(r => r.ItemId <= 0 || r.WarehouseId <= 0))
            {
                return BadRequest(new ResponseResult(false, "Invalid ItemId or WarehouseId in one or more entries.", null));
            }

            var affectedRows = await _service.MarkAsTransferredBulkAsync(requests);

            if (affectedRows > 0)
            {
                return Ok(new ResponseResult(true, $"{affectedRows} record(s) marked as transferred.", affectedRows));
            }

            return NotFound(new ResponseResult(false, "No matching records found to update.", 0));
        }


        [HttpGet("GetAllStockTransferedList")]
        public async Task<IActionResult> GetAllStockTransferedList()
        {
            var list = await _service.GetAllStockTransferedList();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }

        [HttpGet("GetStockTransferedList")]
        public async Task<IActionResult> GetStockTransferedList()
        {
            var list = await _service.GetStockTransferedList();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }


        [HttpPost("AdjustOnHand")]
        public async Task<ActionResult<object>> AdjustOnHand([FromBody] AdjustOnHandRequest req)
        {
            if (req == null) return BadRequest("Invalid payload.");
            if (req.ItemId <= 0 || req.WarehouseId <= 0) return BadRequest("ItemId and WarehouseId are required.");

            try
            {
                var result = await _service.AdjustOnHandAsync(req);
                return Ok(new { data = result, message = "Adjusted." });
            }
            catch (InvalidOperationException ex)
            {
                return BadRequest(new { message = ex.Message });
            }
        }

        [HttpPost("approve-bulk")]
        public async Task<IActionResult> ApproveBulk([FromBody] List<TransferApproveRequest> req)
        {
            try
            {
                await _service.ApproveTransfersBulkAsync(req);
                return Ok(new { message = "Stock transfer approved successfully." });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { message = "Error approving transfers", error = ex.Message });
            }
        }

        [HttpGet("GetByIdStockHistory/{id}")]
        public async Task<IActionResult> GetByIdStockHistory(int id)
        {
            var licenseObj = await _service.GetByIdStockHistory(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpGet("transferred-mr-ids")]
        public async Task<IActionResult> GetTransferredMrIds()
        {
            var ids = await _service.GetTransferredMrIdsAsync();
            return Ok(new { isSuccess = true, data = ids });
        }



        [HttpGet("GetMaterialTransferList")]
        public async Task<IActionResult> GetMaterialTransferList()
        {
            var list = await _service.GetMaterialTransferList();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }
    }
}
