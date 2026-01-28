using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class WarehouseController : ControllerBase
    {
        private readonly IWarehouseService _service;

        public WarehouseController(IWarehouseService service)
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

        [HttpGet("get/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetByIdAsync(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }

        [HttpPost("insert")]
        public async Task<ActionResult> Create(Warehouse warehouse)
        {
            var id = await _service.CreateAsync(warehouse);
            ResponseResult data = new ResponseResult(true, "Warehouse created sucessfully", id);
            return Ok(data);
        }

        [HttpPut("update")]
        public async Task<IActionResult> Update(Warehouse warehouse)
        {
            await _service.UpdateAsync(warehouse);
            ResponseResult data = new ResponseResult(true, "Warehouse updated successfully.", null);
            return Ok(data);
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "Warehouse Deleted sucessfully", null);
            return Ok(data);

        }
        [HttpGet("getBinNameByIdAsync/{id}")]
        public async Task<IActionResult> GetBinNameByIdAsync(int id)
        {
            var binList = await _service.GetBinNameByIdAsync(id);
            ResponseResult data = new ResponseResult(true, "Success", binList);
            return Ok(data);
        }

        [HttpGet("GetNameByWarehouseAsync/{name}")]
        public async Task<IActionResult> GetNameByWarehouseAsync(string name)
        {
            var licenseObj = await _service.GetNameByWarehouseAsync(name);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }
    }
}
