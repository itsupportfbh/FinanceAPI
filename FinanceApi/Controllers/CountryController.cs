using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.Metrics;


namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CountryController : ControllerBase
    {
        private readonly ICountryService _service;

        public CountryController(ICountryService service)
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
        public async Task<ActionResult> Create(Country country)
        {

            var countryName  = await _service.GetByName(country.CountryName);
            if (countryName == null)
            {
                country.CreatedDate = DateTime.Now;
                var id = await _service.CreateAsync(country);
                ResponseResult data = new ResponseResult(true, "Country  created successfully", id);
                return Ok(data);
            }
            else
            {
                ResponseResult data = new ResponseResult(false, "Country  Already Exist.", null);
                return Ok(data);
            }

        }


        [HttpGet("get/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPut("update")]
        public async Task<IActionResult> Update(Country country)
        {
            //await _service.UpdateAsync(country);
            //ResponseResult data = new ResponseResult(true, "Country updated successfully.", null);
            //return Ok(data);

            // Duplicate name check excluding this record
            var exists = await _service.NameExistsAsync(country.CountryName, country.Id);
            if (exists)
            {
                // You can also return Conflict(...) if you prefer a 409 status
                var dup = new ResponseResult(false, "country  name already exists.", null);
                return Ok(dup);
            }

            await _service.UpdateAsync(country);
            var ok = new ResponseResult(true, "country  updated successfully.", null);
            return Ok(ok);
        }



        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "Country Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
