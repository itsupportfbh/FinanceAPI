using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class LocationController : ControllerBase
    {
        private readonly ILocationService _service;

        public LocationController(ILocationService service)
        {
            _service = service;
        }


        [HttpGet("getAllLocation")]
        public async Task<IActionResult> getAllLocation()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }


        [HttpGet("getAllLocationDetails")]
        public async Task<IActionResult> getAllLocationDetails()
        {
            var list = await _service.GetAllLocationDetails();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }



        [HttpGet("getLocationbyId/{id}")]
        public async Task<IActionResult> getLocationbyId(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPost("CreateLocation")]
        public async Task<ActionResult> CreateLocation(Location location)
        {

            var id = await _service.CreateAsync(location);
            ResponseResult data = new ResponseResult(true, "Location created sucessfully", id);
            return Ok(data);

        }

        [HttpPut("updateLocation")]
        public async Task<IActionResult> updateLocation(Location location)
        {
            await _service.UpdateAsync(location);
            ResponseResult data = new ResponseResult(true, "Location updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteLocation/{id}")]
        public async Task<IActionResult> deleteLocation(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "Location Deleted sucessfully", null);
            return Ok(data);
        }

        [HttpGet("getLocationbyCountryId/{id}")]
        public async Task<IActionResult> getLocationbyCountryId(int id)
        {
            var licenseObj = await _service.GetLocationByCountryId(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }

    }
}
