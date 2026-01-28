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
    public class PaymentTermsController : ControllerBase
    {
        private readonly IPaymentTermsService _service;

        public PaymentTermsController(IPaymentTermsService service)
        {
            _service = service;
        }

        [HttpGet("GetPaymentTerms")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "PaymentTerms retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("GetPaymentTermById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var approvalLevel = await _service.GetById(id);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "PaymentTerms not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }




        [HttpPost("CreatePaymentTerm")]
        public async Task<ActionResult> Create(PaymentTerms paymentTerms)
        {
            paymentTerms.CreatedDate = DateTime.Now;
            var id = await _service.CreateAsync(paymentTerms);
            ResponseResult data = new ResponseResult(true, "PaymentTerms created successfully", id);
            return Ok(data);

        }

        [HttpPut("UpdatePaymentTermById/{id}")]
        public async Task<IActionResult> Update(PaymentTerms paymentTerms)
        {
            await _service.UpdateAsync(paymentTerms);
            ResponseResult data = new ResponseResult(true, "PaymentTerms updated successfully.", null);
            return Ok(data);
        }

        [HttpDelete("DeletePaymentTermById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "PaymentTerms Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
