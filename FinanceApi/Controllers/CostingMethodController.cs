using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CostingMethodController : ControllerBase
    {
        private readonly ICostingMethodService _service;

        public CostingMethodController(ICostingMethodService service)
        {
            _service = service;
        }

        [HttpGet("GetAllCostingMethod")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }



        [HttpPost("createCostingMethod")]
        public async Task<ActionResult> Create(CostingMethod costingMethod)
        {

            var id = await _service.CreateAsync(costingMethod);
            ResponseResult data = new ResponseResult(true, "CostingMethod created sucessfully", id);
            return Ok(data);

        }


        [HttpGet("getCostingMethodById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }

        [HttpPut("updateCostingMethodById/{id}")]
        public async Task<IActionResult> Update(CostingMethod costingMethod)
        {
            await _service.UpdateAsync(costingMethod);
            ResponseResult data = new ResponseResult(true, "CostingMethod updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteCostingMethodById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "CostingMethod Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
