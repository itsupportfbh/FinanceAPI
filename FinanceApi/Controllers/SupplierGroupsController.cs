using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;


namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SupplierGroupsController : ControllerBase
    {
        private readonly ISupplierGroupsService _service;

        public SupplierGroupsController(ISupplierGroupsService service)
        {
            _service = service;
        }


        [HttpGet("getAllSupplierGroups")]
        public async Task<IActionResult> getAllSupplierGroups()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }


        [HttpGet("getbySupplierGroups/{id}")]
        public async Task<IActionResult> getbySupplierGroups(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPost("CreateSupplierGroups")]
        public async Task<ActionResult> CreateSupplierGroups(SupplierGroups supplierGroups)
        {

            var id = await _service.CreateAsync(supplierGroups);
            ResponseResult data = new ResponseResult(true, "SupplierGroups created sucessfully", id);
            return Ok(data);

        }

        [HttpPut("updateSupplierGroups")]
        public async Task<IActionResult> updateSupplierGroups(SupplierGroups supplierGroups)
        {
            await _service.UpdateAsync(supplierGroups);
            ResponseResult data = new ResponseResult(true, "SupplierGroups updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteSupplierGroups/{id}")]
        public async Task<IActionResult> deleteSupplierGroups(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "SupplierGroups Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
