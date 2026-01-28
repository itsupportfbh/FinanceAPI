using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MaterialRequisitionController : ControllerBase
    {
        private readonly IMaterialRequisitionService _service;

        public MaterialRequisitionController (IMaterialRequisitionService service)
        {
            _service = service;
        }

        [HttpGet("GetMaterialRequest")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Material Request retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("GetMaterialRequestById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var approvalLevel = await _service.GetById(id);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "Material Request not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }



        [HttpPost("CreateMaterialRequest")]
        public async Task<ActionResult> Create(MaterialRequisition mr)
        {
            mr.CreatedDate = DateTime.Now;
            var id = await _service.CreateAsync(mr);
            ResponseResult data = new ResponseResult(true, "Material Request created successfully", id);
            return Ok(data);

        }

        [HttpPut("UpdateMaterialRequestById/{id}")]
        public async Task<IActionResult> Update(MaterialRequisition mr)
        {
            await _service.UpdateAsync(mr);
            ResponseResult data = new ResponseResult(true, "Material Request updated successfully.", null);
            return Ok(data);
        }

        [HttpDelete("DeleteMaterialRequestById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "Material Request Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
