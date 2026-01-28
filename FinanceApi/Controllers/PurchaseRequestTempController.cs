using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using FinanceApi.Data;


namespace FinanceApi.Controllers
{
   

    [Route("api/[controller]")]
    [ApiController]
    public class PurchaseRequestTempController : ControllerBase
    {
        private readonly IPurchaseRequestTempService _service;

        public PurchaseRequestTempController(IPurchaseRequestTempService service)
        {
            _service = service;
        }

        // GET: api/PurchaseRequestTemp/GetPurchaseRequestTemp
        [HttpGet("GetPurchaseRequestTemp")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.ListAsync();
            var data = new ResponseResult(true, "Purchase Request Drafts retrieved successfully", list);
            return Ok(data);
        }

        // GET: api/PurchaseRequestTemp/GetPurchaseRequestTempById/5
        [HttpGet("GetPurchaseRequestTempById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var draft = await _service.GetByIdAsync(id);
            if (draft == null)
            {
                var notFound = new ResponseResult(false, "Purchase Request Draft not found", null);
                return Ok(notFound);
            }
            var data = new ResponseResult(true, "Success", draft);
            return Ok(data);
        }

        // POST: api/PurchaseRequestTemp/CreatePurchaseRequestTemp
        [HttpPost("CreatePurchaseRequestTemp")]
        public async Task<IActionResult> Create([FromBody] PurchaseRequestTempDto dto)
        {
            var id = await _service.CreateAsync(dto);
            var data = new ResponseResult(true, "Purchase Request Draft created successfully", id);
            return Ok(data);
        }

        // PUT: api/PurchaseRequestTemp/UpdatePurchaseRequestTempById/5
        [HttpPut("UpdatePurchaseRequestTempById/{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] PurchaseRequestTempDto dto)
        {
            dto.Id = id;
            await _service.UpdateAsync(dto);
            var data = new ResponseResult(true, "Purchase Request Draft updated successfully.", null);
            return Ok(data);
        }

        // DELETE: api/PurchaseRequestTemp/DeletePurchaseRequestTempById/5
        [HttpDelete("DeletePurchaseRequestTempById/{id}")]
        public async Task<IActionResult> Delete(int id, [FromQuery] string userId)
        {
            await _service.DeleteAsync(id, userId);
            var data = new ResponseResult(true, "Purchase Request Draft deleted successfully", null);
            return Ok(data);
        }

        [HttpPost("PromotePurchaseRequestTempById/{id}")]
        public async Task<IActionResult> Promote(int id, [FromQuery] string userId)
        {
            try
            {
                var newPrId = await _service.PromoteAsync(id, userId);
                return Ok(new ResponseResult(true, "Draft promoted to Purchase Request successfully", newPrId));
            }
            catch (InvalidOperationException ex) when (ex.Message.StartsWith("Draft not found"))
            {
                return NotFound(new ResponseResult(false, ex.Message, null));
            }
        }

    }


}
