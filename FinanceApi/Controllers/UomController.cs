using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    // Controller (same pattern as CurrencyController)
    [ApiController]
    [Route("api/[controller]")]
    public class UomController : ControllerBase
    {
        private readonly IUomService _service;

        public UomController(IUomService service)
        {
            _service = service;
        }

        [HttpGet("GetUoms")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "UOMs retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("GetUomById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var uom = await _service.GetById(id);
            if (uom == null)
            {
                ResponseResult data1 = new ResponseResult(false, "UOM not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", uom);
            return Ok(data);
        }

        [HttpPost("CreateUom")]
        public async Task<ActionResult> Create(Uom uom)
        {
            uom.CreatedDate = DateTime.Now; // match CurrencyController behavior
            //var id = await _service.CreateAsync(uom);
            //ResponseResult data = new ResponseResult(true, "UOM created successfully", id);
            //return Ok(data);

            var uomName = await _service.GetByName(uom.Name);
            if (uomName == null)
            {
                uom.CreatedDate = DateTime.Now;
                var id = await _service.CreateAsync(uom);
                ResponseResult data = new ResponseResult(true, "UOM  created successfully", id);
                return Ok(data);
            }
            else
            {
                ResponseResult data = new ResponseResult(false, "UOM  Already Exist.", null);
                return Ok(data);
            }
        }

        [HttpPut("UpdateUomById/{id}")]
        public async Task<IActionResult> Update(Uom uom)
        {
            //await _service.UpdateAsync(uom);
            //ResponseResult data = new ResponseResult(true, "UOM updated successfully.", null);
            //return Ok(data);


            var exists = await _service.NameExistsAsync(uom.Name, uom.Id);
            if (exists)
            {
                // You can also return Conflict(...) if you prefer a 409 status
                var dup = new ResponseResult(false, "UOM already exists.", null);
                return Ok(dup);
            }

            await _service.UpdateAsync(uom);
            var ok = new ResponseResult(true, "UOM  updated successfully.", null);
            return Ok(ok);
        }

        [HttpDelete("DeleteUomById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteUom(id);
            ResponseResult data = new ResponseResult(true, "UOM Deleted successfully", null);
            return Ok(data);
        }
    }


}
