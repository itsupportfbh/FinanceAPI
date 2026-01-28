using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DriverController : ControllerBase
    {
        private readonly IDriverService _service;
        public DriverController(IDriverService service)
        {
            _service = service;
        }

        [HttpGet("GetAllDriver")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }

        [HttpPost("createDriver")]
        public async Task<IActionResult> Create(Driver driver)
        {
            var id = await _service.CreateAsync(driver);
            ResponseResult data = new ResponseResult(true, "Driver created successfully", id);
            return Ok(data);
        }

       
        [HttpGet("getDriverById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var driverObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", driverObj);
            return Ok(data);
        }

        
        [HttpPut("updateDriverById/{id}")]
        public async Task<IActionResult> Update(int id, Driver driver)
        {
            driver.Id = id; // important so correct row updates
            await _service.UpdateAsync(driver);
            ResponseResult data = new ResponseResult(true, "Driver updated successfully.", null);
            return Ok(data);
        }

       
        [HttpDelete("deleteDriverById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "Driver deleted successfully", null);
            return Ok(data);
        }
    }
}
