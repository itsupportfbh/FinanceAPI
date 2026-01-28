using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SalesOrderController : ControllerBase
    {
        private readonly ISalesOrderService _service;

        public SalesOrderController(ISalesOrderService service)
        {
            _service = service;
        }

        // GET: api/SalesOrder/getAll
        [HttpGet("getAll")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            var data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }

        // GET: api/SalesOrder/get/5  (SOId-based)
        [HttpGet("get/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var so = await _service.GetByIdAsync(id);
            var data = new ResponseResult(true, "Success", so);
            return Ok(data);
        }

        // POST: api/SalesOrder/insert
        [HttpPost("insert")]
        public async Task<IActionResult> Create([FromBody] SalesOrder salesOrder)
        {
            var id = await _service.CreateAsync(salesOrder);
            var data = new ResponseResult(true, "Sales Order created sucessfully", id);
            return Ok(data);
        }

        // PUT: api/SalesOrder/update?reallocate=true
        // reallocate=true => run full reallocation (WarehouseId/SupplierId/BinId/LockedQty can change)
        [HttpPut("update")]
        public async Task<IActionResult> Update([FromBody] SalesOrder salesOrder, [FromQuery] bool reallocate = false)
        {
            await _service.UpdateAsync(salesOrder, reallocate);
            var data = new ResponseResult(true, "Sales Order updated successfully.", null);
            return Ok(data);
        }

        // DELETE: api/SalesOrder/delete/5?updatedBy=12  (soft delete)
        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id, [FromQuery] int updatedBy)
        {
            await _service.DeactivateAsync(id, updatedBy);
            var data = new ResponseResult(true, "Sales Order Deleted sucessfully", null);
            return Ok(data);
        }

        // GET: api/SalesOrder/GetByQuatitonDetails/123
        [HttpGet("GetByQuatitonDetails/{id}")]
        public async Task<IActionResult> GetByQuatitonDetails(int id)
        {
            var obj = await _service.GetByQuatitonDetails(id);
            var data = new ResponseResult(true, "Success", obj);
            return Ok(data);
        }

        // POST: api/SalesOrder/preview-allocation
        [HttpPost("preview-allocation")]
        public async Task<IActionResult> PreviewAllocation([FromBody] AllocationPreviewRequest req)
        {
            var result = await _service.PreviewAllocationAsync(req);
            var data = new ResponseResult(true, "Success", result);
            return Ok(data);
        }

        [HttpPost("approve/{id:int}")]
        public async Task<IActionResult> Approve(int id, [FromQuery] int approvedBy = 1)
        {
            await _service.ApproveAsync(id, approvedBy);
            var data = new ResponseResult(true, "Approved", null);
            return Ok(data);
        }

        // POST: api/SalesOrder/reject/5
        [HttpPost("reject/{id:int}")]
        public async Task<IActionResult> Reject(int id)
        {
            await _service.RejectAsync(id);
            var data = new ResponseResult(true, "Rejected and unlocked", null);
            return Ok(data);
        }

        [HttpGet("drafts")]
        public async Task<IActionResult> GetDrafts()
        {
            var list = await _service.GetDraftLinesAsync();
            var data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }
        [HttpGet("GetByStatus/{id}")]
        public async Task<IActionResult> GetByStatus(byte id)
        {
            var obj = await _service.GetAllByStatusAsync(id);
            var data = new ResponseResult(true, "Success", obj);
            return Ok(data);
        }
        [HttpGet("customer-open/{customerId:int}")]
        public async Task<IActionResult> GetOpenByCustomer(int customerId)
        {
            if (customerId <= 0)
                return BadRequest("Invalid customerId");

            var list = await _service.GetOpenByCustomerAsync(customerId);

            return Ok(new
            {
                isSuccess = true,
                message = "",
                data = list
            });
        }
    }
}
