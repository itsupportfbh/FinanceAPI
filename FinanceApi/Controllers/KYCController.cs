using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using FinanceApi.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class KYCController : ControllerBase
    {
        private readonly IKycService _kycService;

        public KYCController(IKycService kycService)
        {
            _kycService = kycService;
        }



        [HttpGet("GetAllKYC")]
        public async Task<IActionResult> GetAllKYC()
        {
            var list = await _kycService.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "KYC retrieved successfully", list);
            return Ok(data);
        }


        [HttpGet("GetKYCById/{id}")]
        public async Task<IActionResult> GetKYCById(int id)
        {
            var approvalLevel = await _kycService.GetById(id);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "KYC not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }




        [HttpPost("CreateKYC")]
        public async Task<ActionResult> CreateKYC(KYC KYC)
        {

            KYC.CreatedDate = DateTime.Now;
            var id = await _kycService.CreateAsync(KYC);
            ResponseResult data = new ResponseResult(true, "KYC created successfully", id);
            return Ok(data);


        }

        [HttpPut("UpdateKYCById/{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] KYC KYC)
        {


            await _kycService.UpdateAsync(KYC);
            var ok = new ResponseResult(true, "KYC updated successfully.", null);
            return Ok(ok);
        }


        [HttpDelete("DeleteKYCById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _kycService.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "KYC Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
