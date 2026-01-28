using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class VehicleController : ControllerBase
    {
        private readonly IVehicleService _service;
        public VehicleController(IVehicleService service) => _service = service;

        [HttpGet("GetVehicles")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            var data = new ResponseResult(true, "Vehicles retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("GetVehicleById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var v = await _service.GetByIdAsync(id);
            if (v == null)
                return Ok(new ResponseResult(false, "Vehicle not found", null));

            return Ok(new ResponseResult(true, "Success", v));
        }

        [HttpPost("CreateVehicle")]
        public async Task<IActionResult> Create([FromBody] Vehicle vehicle)
        {
            // CreatedOn is defaulted by SQL; set CreatedBy if you have auth context
            var id = await _service.CreateAsync(vehicle);
            return Ok(new ResponseResult(true, "Vehicle created successfully", id));
        }

        [HttpPut("UpdateVehicleById/{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] Vehicle vehicle)
        {
            vehicle.Id = id;
            await _service.UpdateAsync(vehicle);
            return Ok(new ResponseResult(true, "Vehicle updated successfully.", null));
        }

        [HttpDelete("DeleteVehicleById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeactivateAsync(id);
            return Ok(new ResponseResult(true, "Vehicle deactivated successfully", null));
        }
    }

}
