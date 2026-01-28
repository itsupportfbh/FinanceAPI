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
    public class CatagoryController : ControllerBase
    {
        private readonly ICatagoryService _service;

        public CatagoryController(ICatagoryService service)
        {
            _service = service;
        }


        [HttpGet("GetAllCatagory")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }



        [HttpPost("createCatagory")]
        public async Task<ActionResult> Create(Catagory catagoryDTO)
        {

            var id = await _service.CreateAsync(catagoryDTO);
            ResponseResult data = new ResponseResult(true, "Catagory created sucessfully", id);
            return Ok(data);

        }


        [HttpGet("getCatagoryById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }

        [HttpPut("updateCatagoryById/{id}")]
        public async Task<IActionResult> Update(CatagoryDTO catagoryDTO)
        {
            await _service.Update(catagoryDTO);
            ResponseResult data = new ResponseResult(true, "Catagory updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteCatagoryById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.Delete(id);
            ResponseResult data = new ResponseResult(true, "Catagory Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
