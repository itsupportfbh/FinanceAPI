using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.Metrics;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class StateController : ControllerBase
    {
        private readonly IStateService _service;

        public StateController(IStateService service)
        {
            _service = service;
        }


        [HttpGet("getAllState")]
        public async Task<IActionResult> getAllState()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }


        [HttpGet("getbyIdState/{id}")]
        public async Task<IActionResult> getbyIdState(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPost("CreateState")]
        public async Task<ActionResult> CreateState(State state)
        {

            //var id = await _service.CreateAsync(state);
            //ResponseResult data = new ResponseResult(true, "State created sucessfully", id);
            //return Ok(data);

            var stateName = await _service.GetByName(state.StateName);
            if (stateName == null)
            {
                state.CreatedDate = DateTime.Now;
                var id = await _service.CreateAsync(state);
                ResponseResult data = new ResponseResult(true, "state  created successfully", id);
                return Ok(data);
            }
            else
            {
                ResponseResult data = new ResponseResult(false, "state  Already Exist.", null);
                return Ok(data);
            }

        }

        [HttpPut("updateState")]
        public async Task<IActionResult> updateState(State state)
        {
            //await _service.UpdateAsync(state);
            //ResponseResult data = new ResponseResult(true, "State updated successfully.", null);
            //return Ok(data);



            var exists = await _service.NameExistsAsync(state.StateName, state.Id);
            if (exists)
            {
                // You can also return Conflict(...) if you prefer a 409 status
                var dup = new ResponseResult(false, "StateName already exists.", null);
                return Ok(dup);
            }

            await _service.UpdateAsync(state);
            var ok = new ResponseResult(true, "State  updated successfully.", null);
            return Ok(ok);
        }



        [HttpDelete("deleteState/{id}")]
        public async Task<IActionResult> deleteState(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "State Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
