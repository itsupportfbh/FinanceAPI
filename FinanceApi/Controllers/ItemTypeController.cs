using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ItemTypeController : ControllerBase
    {
        private readonly IItemTypeService _itemTypeService;

        public ItemTypeController (IItemTypeService itemTypeService)
        {
            _itemTypeService = itemTypeService;
        }


        [HttpGet("GetItemType")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _itemTypeService.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "ItemType retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("GetItemTypeById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var approvalLevel = await _itemTypeService.GetById(id);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "ItemType not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }




        [HttpPost("CreateItemType")]
        public async Task<ActionResult> Create(ItemType ItemTypeDTO)
        {

                ItemTypeDTO.CreatedDate = DateTime.Now;
                var id = await _itemTypeService.CreateAsync(ItemTypeDTO);
                ResponseResult data = new ResponseResult(true, "ItemType  created successfully", id);
                return Ok(data);

        }

        [HttpPut("UpdateItemTypeById/{id}")]
        public async Task<IActionResult> Update(ItemType ItemTypeDTO)
        {
  

            await _itemTypeService.UpdateAsync(ItemTypeDTO);
            var ok = new ResponseResult(true, "ItemType  updated successfully.", null);
            return Ok(ok);
        }

        [HttpDelete("DeleteItemTypeById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _itemTypeService.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "ItemType Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
