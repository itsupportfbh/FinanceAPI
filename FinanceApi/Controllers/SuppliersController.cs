using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SuppliersController : ControllerBase
    {
        private readonly ISuppliersService _service;

        public SuppliersController(ISuppliersService service)
        {
            _service = service;
        }

        [HttpGet("getAllSupplier")]
        public async Task<IActionResult> getAllSupplier()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Supplier retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("getSupplierbyId/{id}")]
        public async Task<IActionResult> getSupplierbyId(int id)
        {
            var approvalLevel = await _service.GetById(id);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "Supplier not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }




        [HttpPost("CreateSupplier")]
        public async Task<ActionResult> CreateSupplier(Suppliers supplier)
        {
            supplier.CreatedDate = DateTime.Now;
            supplier.UpdatedDate = DateTime.Now;
            supplier.IsActive = true;

            // Validate ItemID format if needed
            if (!string.IsNullOrEmpty(supplier.ItemID))
            {
                // Optional: validation, trimming whitespace
                supplier.ItemID = supplier.ItemID.Trim();
            }

            var id = await _service.CreateAsync(supplier);
            var result = new ResponseResult(true, "Supplier created successfully", id);
            return Ok(result);
        }


        [HttpPut("updateSupplier")]
        public async Task<IActionResult> updateSupplier(Suppliers suppliers)
        {
            await _service.UpdateAsync(suppliers);
            ResponseResult data = new ResponseResult(true, "Supplier updated successfully.", null);
            return Ok(data);
        }

        [HttpDelete("deleteSupplier/{id}")]
        public async Task<IActionResult> deleteSupplier(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "Supplier Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
