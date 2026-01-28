using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class RecurringController : ControllerBase
    {
        private readonly IRecurringService _service;

        public RecurringController(IRecurringService service)
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
        public async Task<ActionResult> Create(Recurring recurring)
        {
            var recurringName = await _service.GetByName(recurring.RecurringName);
            if (recurringName == null)
            {
                var id = await _service.CreateAsync(recurring);
                ResponseResult data = new ResponseResult(true, "Recurring created sucessfully", id);
                return Ok(data);
            }
            else
            {
                ResponseResult data = new ResponseResult(true, "Recurring  Already Exist", null);
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
        public async Task<IActionResult> Update(Recurring recurring)
        {
            var isRecurringExist = await _service.GetByName(recurring.RecurringName);
            if (isRecurringExist != null && isRecurringExist.Id != recurring.Id)
            {
                ResponseResult responseResult = new ResponseResult(false, "Recurring Already Exists", recurring);
                return Ok(responseResult);

            }
            await _service.UpdateAsync(recurring);
            ResponseResult data = new ResponseResult(true, "Recurring updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "Recurring Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
