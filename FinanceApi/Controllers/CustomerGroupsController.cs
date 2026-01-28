using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CustomerGroupsController : ControllerBase
    {
        private readonly ICustomerGroupsService _service;

        public CustomerGroupsController(ICustomerGroupsService service)
        {
            _service = service;
        }

        

        [HttpGet("getAllCustomerGroups")]
        public async Task<IActionResult> getAllCustomerGroups()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }


        [HttpGet("getbyCustomerGroups/{id}")]
        public async Task<IActionResult> getbyCustomerGroups(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPost("CreateCustomerGroups")]
        public async Task<ActionResult> CreateCustomerGroups(CustomerGroups customerGroups)
        {

            var id = await _service.CreateAsync(customerGroups);
            ResponseResult data = new ResponseResult(true, "CustomerGroups created sucessfully", id);
            return Ok(data);

        }

        [HttpPut("updateCustomerGroups")]
        public async Task<IActionResult> updateCustomerGroups(CustomerGroups customerGroups)
        {
            await _service.UpdateAsync(customerGroups);
            ResponseResult data = new ResponseResult(true, "CustomerGroups updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteCustomerGroups/{id}")]
        public async Task<IActionResult> deleteCustomerGroups(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "CustomerGroups Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
