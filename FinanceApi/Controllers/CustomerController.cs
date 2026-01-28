using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;


namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CustomerController : ControllerBase
    {
        private readonly ICustomerService _service;

        public CustomerController (ICustomerService service)
        {
            _service = service;
        }

        [HttpGet("getAll")]
        public async Task<ActionResult<List<CustomerDto>>> GetAll()
        {
            return Ok(await _service.GetAllAsync());
        }

        [HttpGet("get/{id}")]        
        public async Task<ActionResult<CustomerDto>> GetById(int id)
        {
            var customer = await _service.GetByIdAsync(id);
            if (customer == null) return NotFound();
            return Ok(customer);
        }

        [HttpPost("insert")]
        public async Task<ActionResult<Customer>> Create(Customer customer)
        {
            try
            {
                var created = await _service.CreateAsync(customer);
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
        public async Task<ActionResult<Customer>> Update(int id,Customer customer)
        {
            var updated = await _service.UpdateAsync(id, customer);
            if (updated == null) return NotFound();
            return (updated);
        }

        [HttpDelete("delete/{id}")]
        public async Task<ActionResult<Customer>> Delete(int id)
        {
            var deleted = await _service.DeleteAsync(id);
            if (!deleted) return NotFound();
            return NoContent();

        }

     
    }
}
