using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class StrategyController : ControllerBase
    {
        private readonly IStrategyService _service;

        public StrategyController(IStrategyService service)
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
        public async Task<ActionResult> Create(Strategy strategy)
        {

            var id = await _service.CreateAsync(strategy);
            ResponseResult data = new ResponseResult(true, "Strategy created sucessfully", id);
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
        public async Task<IActionResult> Update(Strategy strategy)
        {
            await _service.UpdateAsync(strategy);
            ResponseResult data = new ResponseResult(true, "Strategy updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "Strategy Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
