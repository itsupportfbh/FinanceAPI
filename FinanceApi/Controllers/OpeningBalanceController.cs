using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class OpeningBalanceController : ControllerBase
    {
        private readonly IOpeningBalanceService _service;

        public OpeningBalanceController(IOpeningBalanceService service)
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



        [HttpPost("insert")]
        public async Task<ActionResult> Create(OpeningBalance openingBalance)
        {

            var id = await _service.CreateAsync(openingBalance);
            ResponseResult data = new ResponseResult(true, "OpeningBalance created sucessfully", id);
            return Ok(data);

        }


        [HttpGet("get/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPut("update")]
        public async Task<IActionResult> Update(OpeningBalance openingBalance)
        {
            await _service.UpdateAsync(openingBalance);
            ResponseResult data = new ResponseResult(true, "OpeningBalance updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("Delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "OpeningBalance Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
