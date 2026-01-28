using FinanceApi.Interfaces;
using FinanceApi.Models;
using FinanceApi.Services;
using Microsoft.AspNetCore.Mvc;


namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class PurchaseController : ControllerBase
    {
        private readonly IPurchaseService _service;

        public PurchaseController(IPurchaseService service)
        {
            _service = service;
        }

        [HttpGet("getAll")]
        public async Task<ActionResult<List<PurchasesDto>>> GetAll()
        {
            return Ok(await _service.GetAllAsync());
        }

        [HttpGet("get/{id}")]
        public async Task<ActionResult<PurchasesDto>> GetById(int id)
        {
            var purchase = await _service.GetByIdAsync(id);
            if (purchase == null) return NotFound();
            return Ok(purchase);
        }

        [HttpPost("insert")]
        public async Task<ActionResult<Purchases>> Create(Purchases purchase)
        {
            try
            {
                var created = await _service.CreateAsync(purchase);
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
        public async Task<ActionResult<Purchases>> Update(int id, Purchases purchases)
        {
            var updated = await _service.UpdateAsync(id, purchases);
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
