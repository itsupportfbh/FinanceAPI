using System.Diagnostics.Metrics;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class RegionController : ControllerBase
    {
        private readonly IRegionService _service;

        public RegionController(IRegionService service)
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

            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }

        [HttpPost("insert")]
        public async Task<ActionResult> Create(Region region)
        {
            var id = await _service.CreateAsync(region);
            ResponseResult data = new ResponseResult(true, "Region created sucessfully", id);
            return Ok(data);

        }

        [HttpPut("update")]
        public async Task<IActionResult> Update(Region region)
        {
            await _service.UpdateAsync(region);
            ResponseResult data = new ResponseResult(true, "Region updated successfully.", null);
            return Ok(data);
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "Region Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
