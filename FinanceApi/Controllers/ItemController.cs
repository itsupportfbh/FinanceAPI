using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
   

    [ApiController]
    [Route("api/[controller]")]
    public class ItemController : ControllerBase
    {
        private readonly IItemService _service;
        public ItemController(IItemService service) => _service = service;

        [HttpGet("GetItems")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            var data = new ResponseResult(true, "Items retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("GetItemById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var item = await _service.GetById(id);
            if (item == null)
                return Ok(new ResponseResult(false, "Item not found", null));

            return Ok(new ResponseResult(true, "Success", item));
        }

        [HttpPost("CreateItem")]
        public async Task<IActionResult> Create([FromBody] Item item)
        {
            try
            {
                var newId = await _service.CreateAsync(item);
                return Ok(new ResponseResult(true, "Item created successfully", newId));
            }
            catch (Exception ex)
            {
                return StatusCode(500, new ResponseResult(false, "Error creating item: " + ex.Message, null));
            }
        }

        [HttpPut("UpdateItemById/{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] Item item)
        {
            item.Id = id;                         
            item.UpdatedDate = DateTime.UtcNow;

            await _service.UpdateAsync(item);
            return Ok(new ResponseResult(true, "Item updated successfully.", null));
        }


        [HttpDelete("DeleteItemById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteItem(id);
            return Ok(new ResponseResult(true, "Item deleted successfully", null));
        }

        [HttpGet("CheckItemExists/{itemCode}")]
        public async Task<IActionResult> CheckItemExists(string itemCode)
        {
            if (string.IsNullOrWhiteSpace(itemCode))
                return BadRequest(new ResponseResult(false, "Invalid item code", null));

            var exists = await _service.ExistsInItemMasterAsync(itemCode);
            return Ok(new ResponseResult(true, exists ? "Item exists" : "Item not found", exists));
        }

    }

}
