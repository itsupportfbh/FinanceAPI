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
    public class ApprovalLevelcontroller : ControllerBase
    {
        private readonly IApprovallevelSevice _service;

        public ApprovalLevelcontroller(IApprovallevelSevice service)
        {
            _service = service;
        }
  

        [HttpGet("GetApprovalLevels")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Approval levels retrieved successfully", list);
            return Ok(data);
        }
   

        [HttpGet("GetApprovalLevelById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var approvalLevel = await _service.GetById(id);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "Approval level not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }

     


        [HttpPost("CreateApprovalLevel")]
        public async Task<ActionResult> Create(ApprovalLevel approvalLevel)
        {
            var approvalName = await _service.GetByName(approvalLevel.Name);
            if (approvalName == null)
            {
                approvalLevel.CreatedDate = DateTime.Now;
                var id = await _service.CreateAsync(approvalLevel);
                ResponseResult data = new ResponseResult(true, "Approval level created successfully", id);
                return Ok(data);
            }
            else
            {
                ResponseResult data = new ResponseResult(false, "Approval level Already Exist.", null);
                return Ok(data);
            }

        }

        [HttpPut("UpdateApprovalLevelById/{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] ApprovalLevel approvalLevel)
        {
            if (id != approvalLevel.Id)
                return BadRequest(new ResponseResult(false, "Route id and body id do not match.", null));

            // Duplicate name check excluding this record
            var exists = await _service.NameExistsAsync(approvalLevel.Name, approvalLevel.Id);
            if (exists)
            {
                // You can also return Conflict(...) if you prefer a 409 status
                var dup = new ResponseResult(false, "Approval level name already exists.", null);
                return Ok(dup);
            }

            await _service.UpdateAsync(approvalLevel);
            var ok = new ResponseResult(true, "Approval level updated successfully.", null);
            return Ok(ok);
        }


        [HttpDelete("DeleteApprovalLevelById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "Approval level Deleted sucessfully", null);
            return Ok(data);
        }
        [HttpGet("GetApprovalLevelByName/{name}")]
        public async Task<IActionResult> GetByName(string name)
        {
            var approvalLevel = await _service.GetByName(name);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "Approval level not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }
    }
}
