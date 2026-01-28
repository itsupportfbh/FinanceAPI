using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ItemSetController : ControllerBase
    {
        private readonly IItemSetService _service;
        public ItemSetController(IItemSetService service)
        {
            _service = service;
        }


        [HttpGet("GetAllItemSet")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }



        [HttpPost("createItemSet")]
        public async Task<ActionResult> Create(ItemSet ItemSetDTO)
        {

            var id = await _service.CreateAsync(ItemSetDTO);
            ResponseResult data = new ResponseResult(true, "ItemSet created sucessfully", id);
            return Ok(data);

        }



        [HttpGet("getItemSetById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPut("updateItemSetById/{id}")]
        public async Task<IActionResult> Update(ItemSet ItemSetDTO)
        {
            await _service.UpdateAsync(ItemSetDTO);
            ResponseResult data = new ResponseResult(true, "ItemSet updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteItemSetById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "ItemSet Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
