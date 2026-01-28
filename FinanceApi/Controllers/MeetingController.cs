using FinanceApi.Interfaces;
using FinanceApi.Models;
using FinanceApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MeetingController : ControllerBase
    {
        private readonly IMeetingService _service;

        public MeetingController(IMeetingService service)
        {
            _service = service;
        }

        [HttpGet("getAll")]
        public async Task<ActionResult<List<Meeting>>> GetAll()
        {
            return Ok(await _service.GetAllAsync());
        }

        [HttpGet("get/{id}")]
        public async Task<ActionResult<Meeting>> GetById(int id)
        {
            var sale = await _service.GetByIdAsync(id);
            if (sale == null) return NotFound();
            return Ok(sale);
        }

        [HttpPost("insert")]
        public async Task<ActionResult<Meeting>> Create(Meeting meeting)
        {
            try
            {
                var created = await _service.CreateAsync(meeting);
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
        public async Task<ActionResult<Meeting>> Update(int id, Meeting meeting)
        {
            var updated = await _service.UpdateAsync(id, meeting);
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
