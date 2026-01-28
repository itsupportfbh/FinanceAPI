using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading.Tasks;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ItemMasterController : ControllerBase
    {
        private readonly IItemMasterService _service;
        public ItemMasterController(IItemMasterService service) => _service = service;

        [HttpGet("GetItems")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            return Ok(new ResponseResult(true, "Items retrieved successfully", list));
        }

        [HttpGet("GetItemById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var item = await _service.GetByIdAsync(id);
            if (item == null)
                return Ok(new ResponseResult(false, "Item not found", null));

            return Ok(new ResponseResult(true, "Success", item));
        }

        [HttpPost("CreateItem")]
        public async Task<IActionResult> Create([FromBody] ItemMasterUpsertDto dto)
        {
            if (dto == null)
                return Ok(new ResponseResult(false, "Request body is required", null));
            if (string.IsNullOrWhiteSpace(dto.Sku) || string.IsNullOrWhiteSpace(dto.Name))
                return Ok(new ResponseResult(false, "SKU and Name are required", null));

            try
            {
                var id = await _service.CreateAsync(dto);
                return Ok(new ResponseResult(true, "Item created successfully", id));
            }
            catch (Exception ex)
            {
                return Ok(new ResponseResult(false, "Create failed", ex.Message));
            }
        }

        [HttpPut("UpdateItemById/{id}")]
        public async Task<IActionResult> Update(int id, [FromBody] ItemMasterUpsertDto dto)
        {
            if (dto == null)
                return Ok(new ResponseResult(false, "Request body is required", null));
            if (string.IsNullOrWhiteSpace(dto.Sku) || string.IsNullOrWhiteSpace(dto.Name))
                return Ok(new ResponseResult(false, "SKU and Name are required", null));

            try
            {
                dto.Id = id;
                await _service.UpdateAsync(dto);
                return Ok(new ResponseResult(true, "Item updated successfully", null));
            }
            catch (Exception ex)
            {
                return Ok(new ResponseResult(false, "Update failed", ex.Message));
            }
        }

        [HttpDelete("DeleteItemById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            try
            {
                await _service.DeleteAsync(id);
                return Ok(new ResponseResult(true, "Item deleted successfully", null));
            }
            catch (Exception ex)
            {
                return Ok(new ResponseResult(false, "Delete failed", ex.Message));
            }
        }

        [HttpGet("Audit/{itemId}")]
        public async Task<IActionResult> GetAudits(int itemId)
        {
            var item = await _service.getAuditByItemId(itemId);
            if (item == null)
                return Ok(new ResponseResult(false, "ItemAudit not found", null));

            return Ok(new ResponseResult(true, "Success", item));
        }

        [HttpGet("GetWarehouse/{itemId}")]
        public async Task<IActionResult> GetWarehouse(int itemId)
        {
            var item = await _service.getStockByItemId(itemId);
            if (item == null)
                return Ok(new ResponseResult(false, "ItemWareHouse not found", null));

            return Ok(new ResponseResult(true, "Success", item));
        }

        [HttpGet("GetSupplier/{itemId}")]
        public async Task<IActionResult> GetSupplier(int itemId)
        {
            var item = await _service.getPriceByItemId(itemId);
            if (item == null)
                return Ok(new ResponseResult(false, "ItemPrice not found", null));

            return Ok(new ResponseResult(true, "Success", item));
        }

        [HttpGet("GetBom/{itemId}")]
        public async Task<IActionResult> GetBom(int itemId)
        {
            var item = await _service.GetBomSnapshot(itemId);
            if (item == null)
                return Ok(new ResponseResult(false, "ItemBom not found", null));

            return Ok(new ResponseResult(true, "Success", item));
        }

        // ✅ Apply GRN to inventory (you had this; leaving as-is)
        [HttpPost("ApplyGrn")]
        public async Task<IActionResult> ApplyGrn([FromBody] ApplyGrnRequest req)
        {
            if (req is null || req.Lines is null || req.Lines.Count == 0)
                return Ok(new ResponseResult(false, "Empty GRN payload", null));

            try
            {
                await _service.ApplyGrnToInventoryAsync(req);
                return Ok(new ResponseResult(true, "GRN applied to inventory", null));
            }
            catch (Exception ex)
            {
                return Ok(new ResponseResult(false, "Apply GRN failed", ex.Message));
            }
        }

        // ✅ New endpoint to post warehouse + supplier price together (e.g., from GRN)
        [HttpPost("UpdateWarehouseAndSupplierPrice")]
        public async Task<IActionResult> UpdateWarehouseAndSupplierPrice([FromBody] UpdateWarehouseSupplierPriceDto dto)
        {
            if (dto == null || string.IsNullOrWhiteSpace(dto.ItemCode))
                return Ok(new ResponseResult(false, "ItemCode (SKU) is required", null));

            try
            {
                await _service.UpdateWarehouseAndSupplierPriceAsync(dto);
                return Ok(new ResponseResult(true, "Warehouse stock and supplier price updated", null));
            }
            catch (Exception ex)
            {
                return Ok(new ResponseResult(false, "Update failed", ex.Message));
            }
        }


        [HttpGet("GetItemDetailsByItemId/{id}")]
        public async Task<IActionResult> GetItemDetailsByItemId(int id)
        {
            var item = await _service.GetItemDetailsByItemId(id);
            if (item == null)
                return Ok(new ResponseResult(false, "ItemWareHouse not found", null));

            return Ok(new ResponseResult(true, "Success", item));
        }

    }
}
