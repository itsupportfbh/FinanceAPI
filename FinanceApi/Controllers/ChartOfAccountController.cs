using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    using FinanceApi.Data;
    using FinanceApi.InterfaceService;
    using Microsoft.AspNetCore.Mvc;

    [ApiController]
    [Route("api/[controller]")]
    public class ChartOfAccountController : ControllerBase
    {
        private readonly IChartOfAccountService _service;

        public ChartOfAccountController(IChartOfAccountService service)
        {
            _service = service;
        }

        [HttpGet("GetChartOfAccounts")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            var data = new ResponseResult(true, "Chart of Accounts retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("GetChartOfAccountById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var coa = await _service.GetById(id);
            if (coa == null)
            {
                var notFound = new ResponseResult(false, "Chart of Account not found", null);
                return Ok(notFound);
            }
            var data = new ResponseResult(true, "Success", coa);
            return Ok(data);
        }

        [HttpPost("CreateChartOfAccount")]
        public async Task<IActionResult> Create(ChartOfAccount coa)
        {
            coa.CreatedDate = DateTime.Now; // match Currency pattern
            var id = await _service.CreateAsync(coa);
            var data = new ResponseResult(true, "Chart of Account created successfully", id);
            return Ok(data);
        }

        [HttpPut("UpdateChartOfAccountById/{id}")]
        public async Task<IActionResult> Update(ChartOfAccount coa)
        {
            await _service.UpdateAsync(coa);
            var data = new ResponseResult(true, "Chart of Account updated successfully.", null);
            return Ok(data);
        }

        [HttpDelete("DeleteChartOfAccountById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteChartOfAccount(id);
            var data = new ResponseResult(true, "Chart of Account deleted successfully", null);
            return Ok(data);
        }
    }

}
