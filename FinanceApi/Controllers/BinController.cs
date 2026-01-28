using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BinController : ControllerBase
    {
        private readonly IBinServices _service;
        public BinController(IBinServices service)
        {
            _service = service;
        }


        [HttpGet("GetAllBin")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }



        [HttpPost("createBin")]
        public async Task<ActionResult> Create(Bin BinDTO)
        {

            var id = await _service.CreateAsync(BinDTO);
            ResponseResult data = new ResponseResult(true, "Bin created sucessfully", id);
            return Ok(data);

        }



        [HttpGet("getBinById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPut("updateBinById/{id}")]
        public async Task<IActionResult> Update(Bin BinDTO)
        {
            await _service.UpdateAsync(BinDTO);
            ResponseResult data = new ResponseResult(true, "Bin updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteBinById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "Bin Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
