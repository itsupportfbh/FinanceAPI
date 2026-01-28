using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ServiceController : ControllerBase
    {
        private readonly IServicesService _service;

        public ServiceController(IServicesService service)
        {
            _service = service; 
        }

        [HttpGet("getAllService")]
        public async Task<IActionResult> getAllService()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }


        [HttpGet("getbyIdService/{id}")]
        public async Task<IActionResult> getbyIdService(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPost("CreateService")]
        public async Task<ActionResult> CreateService(Service service)
        {

            var id = await _service.CreateAsync(service);
            ResponseResult data = new ResponseResult(true, "Service created sucessfully", id);
            return Ok(data);

        }

        [HttpPut("updateService")]
        public async Task<IActionResult> updateService(Service service)
        {
            await _service.UpdateAsync(service);
            ResponseResult data = new ResponseResult(true, "Service updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteService/{id}")]
        public async Task<IActionResult> deleteService(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "Service Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
