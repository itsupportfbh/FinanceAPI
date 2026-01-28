using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SupplierController : ControllerBase
    {
        private readonly ISupplierService _service;

        public SupplierController(ISupplierService service)
        {
            _service = service;
        }

        [HttpGet("getAll")]
        public async Task<ActionResult<List<SupplierDto>>> GetAll()
        {
            return Ok(await _service.GetAllAsync());
        }

        [HttpGet("get/{id}")]
        public async Task<ActionResult<SupplierDto>> GetById(int id)
        {
            var supplier = await _service.GetByIdAsync(id);
            if (supplier == null) return NotFound();
            return Ok(supplier);
        }

        [HttpPost("insert")]
        public async Task<ActionResult<Supplier>> Create(Supplier supplier)
        {
            try
            {
                var created = await _service.CreateAsync(supplier);
                return CreatedAtAction(nameof(GetById), new { id = created.Id }, created);
            }
            catch (ArgumentException ex)
            {
                return BadRequest(ex.Message);
            }
            catch (Exception ex)
            {
                return StatusCode(500, "An unexpected error occurred: " + ex.Message);
            }
        }

        [HttpPut("update/{id}")]
        public async Task<ActionResult<Supplier>> Update(int id, Supplier supplier)
        {
            var updated = await _service.UpdateAsync(id, supplier);
            if (updated == null) return NotFound();
            return (updated);
        }

        [HttpDelete("delete/{id}")]
        public async Task<ActionResult<Supplier>> Delete(int id)
        {
            var deleted = await _service.DeleteAsync(id);
            if (!deleted) return NotFound();
            return NoContent();

        }
    }
}
