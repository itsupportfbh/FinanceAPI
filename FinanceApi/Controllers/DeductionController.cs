using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DeductionController : ControllerBase
    {
        private readonly IDeductionService _service;

        public DeductionController(IDeductionService service)
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
        public async Task<ActionResult> Create(Deduction deduction)
        {
            var id = await _service.CreateAsync(deduction);
            ResponseResult data = new ResponseResult(true, "Deduction created sucessfully", id);
            return Ok(data);
        }

        [HttpPut("update")]
        public async Task<IActionResult> Update(Deduction deduction)
        {
            await _service.UpdateAsync(deduction);
            ResponseResult data = new ResponseResult(true, "Deduction updated successfully.", null);
            return Ok(data);
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "Deduction Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
