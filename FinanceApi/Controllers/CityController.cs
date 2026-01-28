using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CityController : ControllerBase
    {
        private readonly ICityService _service;

        public CityController(ICityService service)
        {
            _service = service;
        }

        [HttpGet("GetAllCities")]
        public async Task<IActionResult> getAllCitites()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }


        [HttpGet("GetByIdCities/{id}")]
        public async Task<IActionResult> getbyIdCitites(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPost("CreateCities")]
        public async Task<ActionResult> CreateCities([FromBody] City city)
        {
            var name = city?.CityName?.Trim();
            if (string.IsNullOrWhiteSpace(name))
                return BadRequest(new ResponseResult(false, "City name is required.", null));

            if (city.CountryId <= 0)
                return BadRequest(new ResponseResult(false, "Country is required.", null));

            // Debug guard: ensure we see what we're sending
            // _logger.LogInformation("CreateCities name={Name}, countryId={CountryId}", name, city.CountryId);

            var existsInCountry = await _service.NameExistsAsync(name, city.CountryId, excludeId: 0);
            if (existsInCountry)
                return Ok(new ResponseResult(false, "City name already exists in this country.", null));

            city.CityName = name;
            var id = await _service.CreateAsync(city);
            return Ok(new ResponseResult(true, "City created successfully.", id));
        }

        [HttpPut("updateCities")]
        public async Task<IActionResult> updateCities([FromBody] City city)
        {
            if (city == null || city.Id <= 0)
                return BadRequest(new ResponseResult(false, "Invalid city id.", null));

            var name = city.CityName?.Trim();
            if (string.IsNullOrWhiteSpace(name))
                return BadRequest(new ResponseResult(false, "City name is required.", null));

            if (city.CountryId <= 0)
                return BadRequest(new ResponseResult(false, "Country is required.", null));

            // _logger.LogInformation("UpdateCities id={Id}, name={Name}, countryId={CountryId}", city.Id, name, city.CountryId);

            var existsInCountry = await _service.NameExistsAsync(name, city.CountryId, excludeId: city.Id);
            if (existsInCountry)
                return Ok(new ResponseResult(false, "City name already exists in this country.", null));

            city.CityName = name;
            await _service.UpdateAsync(city);
            return Ok(new ResponseResult(true, "City updated successfully.", null));
        }


        [HttpDelete("deleteCities/{id}")]
        public async Task<IActionResult> deleteCities(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "City Deleted sucessfully", null);
            return Ok(data);
        }


        [HttpGet("GetStateWithCountryId/{id}")]
        public async Task<IActionResult> GetStateWithCountryId(int id)
        {
            var licenseObj = await _service.GetStateWithCountryId(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpGet("GetCityWithStateId/{id}")]
        public async Task<IActionResult> GetCityWithStateId(int id)
        {
            var licenseObj = await _service.GetCityWithStateId(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }

    }
}
