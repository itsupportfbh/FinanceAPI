using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class FlagIssuesController : ControllerBase
    {
        private readonly IflagIssuesServices _service;
        public FlagIssuesController(IflagIssuesServices service)
        {
            _service = service;
        }


        [HttpGet("GetAllFlagissue")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }



        [HttpPost("createFlagissue")]
        public async Task<ActionResult> Create(FlagIssues flagIssuesDTO)
        {

            //var id = await _service.CreateAsync(flagIssuesDTO);
            //ResponseResult data = new ResponseResult(true, "FlagIssues created sucessfully", id);
            //return Ok(data);

            var flagissuesName = await _service.GetByName(flagIssuesDTO.FlagIssuesNames);
            if (flagissuesName == null)
            {
                flagIssuesDTO.CreatedDate = DateTime.Now;
                var id = await _service.CreateAsync(flagIssuesDTO);
                ResponseResult data = new ResponseResult(true, "Flag Issues  created successfully", id);
                return Ok(data);
            }
            else
            {
                ResponseResult data = new ResponseResult(false, "Flag Issues  Already Exist.", null);
                return Ok(data);
            }
        }


       
        [HttpGet("getFlagissueById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPut("updateFlagissueById/{id}")]
        public async Task<IActionResult> Update(FlagIssues flagIssuesDTO)
        {
            //await _service.UpdateAsync(flagIssuesDTO);
            //ResponseResult data = new ResponseResult(true, "FlagIssues updated successfully.", null);
            //return Ok(data);

            var exists = await _service.NameExistsAsync(flagIssuesDTO.FlagIssuesNames, flagIssuesDTO.ID);
            if (exists)
            {
                // You can also return Conflict(...) if you prefer a 409 status
                var dup = new ResponseResult(false, "flagIssuesname already exists.", null);
                return Ok(dup);
            }

            await _service.UpdateAsync(flagIssuesDTO);
            var ok = new ResponseResult(true, "flagIssuesname  updated successfully.", null);
            return Ok(ok);
        }



        [HttpDelete("deleteFlagissueById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "FlagIssues Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
