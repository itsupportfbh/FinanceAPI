using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using FinanceApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{

    [ApiController]
    [Route("api/[controller]")]
    public class PurchaseOrderController : ControllerBase
    {
        private readonly IPurchaseOrderService _service;

        public PurchaseOrderController(IPurchaseOrderService service)
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

        [HttpGet("GetAllDetailswithGRN")]
        public async Task<IActionResult> GetAllDetailswithGRN()
        {
            var list = await _service.GetAllDetailswithGRN();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }

        [HttpGet("get/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetByIdAsync(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }

        [HttpPost("insert")]
        public async Task<ActionResult> Create(PurchaseOrder purchaseOrder)
        {
            var id = await _service.CreateAsync(purchaseOrder);
            ResponseResult data = new ResponseResult(true, "PurchaseOrder created sucessfully", id);
            return Ok(data);
        }

        [HttpPut("update")]
        public async Task<IActionResult> Update(PurchaseOrder purchaseOrder)
        {
            await _service.UpdateAsync(purchaseOrder);
            ResponseResult data = new ResponseResult(true, "PurchaseOrder updated successfully.", null);
            return Ok(data);
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "PurchaseOrder Deleted sucessfully", null);
            return Ok(data);
        }

        [HttpGet("{poNo}/qr")]
        public ActionResult<PoQrResponse> GetQr(string poNo)
        {
            var res = _service.BuildPoQr(poNo);
            return Ok(res);
        }

        [HttpPost("{poId:int}/email-supplier")]
        [Consumes("multipart/form-data")]
        public async Task<IActionResult> EmailSupplierPo(
             [FromRoute] int poId,
             [FromForm] EmailSupplierPoRequest req)
        {
            var result = await _service.EmailSupplierPoAsync(poId, req.Pdf);
            return result.isSuccess ? Ok(result) : BadRequest(result);
        }

    }
}
