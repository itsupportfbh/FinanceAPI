using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BankController : ControllerBase
    {
        private readonly IBankService _bankService;
        public BankController(IBankService bankService)
        {
            _bankService = bankService;
        }
        [HttpGet("GetAllBank")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _bankService.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }



        [HttpPost("createBank")]
        public async Task<ActionResult> Create(Bank BankDto)
        {

            var id = await _bankService.CreateAsync(BankDto);
            ResponseResult data = new ResponseResult(true, "Bank created sucessfully", id);
            return Ok(data);

        }



        [HttpGet("getBankById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _bankService.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPut("updateBankById/{id}")]
        public async Task<IActionResult> Update(Bank BankDto)
        {
            await _bankService.UpdateAsync(BankDto);
            ResponseResult data = new ResponseResult(true, "Bank updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteBankById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _bankService.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "Bank Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
