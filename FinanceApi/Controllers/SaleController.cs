using FinanceApi.Models;
using FinanceApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class SaleController : ControllerBase
    {
        private readonly ISaleService _service;

        public SaleController(ISaleService service)
        {
            _service = service;
        }

        [HttpGet("getAll")]
        public async Task<ActionResult<List<SalesDto>>> GetAll()
        {
            return Ok(await _service.GetAllAsync());
        }

        [HttpGet("get/{id}")]
        public async Task<ActionResult<SalesDto>> GetById(int id)
        {
            var sale = await _service.GetByIdAsync(id);
            if (sale == null) return NotFound();
            return Ok(sale);
        }

        [HttpPost("insert")]
        public async Task<ActionResult<Sales>> Create(Sales sale)
        {
            try
            {
                var created = await _service.CreateAsync(sale);
                return CreatedAtAction(nameof(GetById), new { id = created.Id }, created);
            }
            catch (ArgumentException ex)
            {
                // Return 400 Bad Request with just the message
                return BadRequest(ex.Message);
            }
            catch (Exception ex)
            {
                // For unexpected exceptions, return 500 Internal Server Error
                return StatusCode(500, "An unexpected error occurred: " + ex.Message);
            }


        }

        [HttpPut("update/{id}")]
        public async Task<ActionResult<Sales>> Update(int id, Sales sale)
        {
            var updated = await _service.UpdateAsync(id, sale);
            if (updated == null) return NotFound();
            return Ok(updated);
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            var deleted = await _service.DeleteAsync(id);
            if (!deleted) return NotFound();
            return NoContent();
        }
    }
}
