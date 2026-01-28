// Controllers/PickingController.cs
using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using FinanceApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class PickingController : ControllerBase
    {
        private readonly IPickingService _service;
        private readonly ICodeImageService _img;

        public PickingController(IPickingService service, ICodeImageService img)
        {
            _service = service;
            _img = img;
        }

        // ---------- CRUD ----------

        [HttpGet("getAll")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            var res = new ResponseResult(true, "Success", list);
            return Ok(res);
        }

        [HttpGet("get/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var dto = await _service.GetByIdAsync(id);
            var res = new ResponseResult(true, "Success", dto);
            return Ok(res);
        }

        [HttpPost("insert")]
        public async Task<IActionResult> Create([FromBody] Picking model)
        {
            var id = await _service.CreateAsync(model);
            var res = new ResponseResult(true, "Picking created successfully", id);
            return Ok(res);
        }

        [HttpPut("update")]
        public async Task<IActionResult> Update([FromBody] Picking model)
        {
            await _service.UpdateAsync(model);
            var res = new ResponseResult(true, "Picking updated successfully", null);
            return Ok(res);
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id, [FromQuery] int updatedBy)
        {
            await _service.DeactivateAsync(id, updatedBy);
            var res = new ResponseResult(true, "Picking deleted successfully", null);
            return Ok(res);
        }

        // ---------- Codes (text) ----------

        


        [HttpPost("codes")]
        public async Task<IActionResult> GenerateCodes([FromBody] CodesRequest req)
        {
            var payload = await _service.GenerateCodesAsync(req);
            return Ok(new ResponseResult(true, "Success", payload));
        }

    }
}
