using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace UnityWorksERP.Finance.AR
{
    [ApiController]
    [Route("api/[controller]")]
    public class ArReceiptController : ControllerBase
    {
        private readonly IArReceiptService _service;

        public ArReceiptController(IArReceiptService service)
        {
            _service = service;
        }

        // GET api/ArReceipt
        [HttpGet("getAll")]
        public async Task<IActionResult> GetList()
        {
            var data = await _service.GetListAsync();
            return Ok(new { data });
        }

        // GET api/ArReceipt/5
        [HttpGet("get/{id:int}")]
        public async Task<IActionResult> GetById(int id)
        {
            var item = await _service.GetByIdAsync(id);
            if (item == null) return NotFound();
            return Ok(item);
        }

        // GET api/ArReceipt/open-invoices/1
        [HttpGet("open-invoices/{customerId:int}")]
        public async Task<IActionResult> GetOpenInvoices(int customerId)
        {
            var data = await _service.GetOpenInvoicesAsync(customerId);
            return Ok(new { data });
        }

        // POST api/ArReceipt
        [HttpPost("insert")]
        public async Task<IActionResult> Create([FromBody] ArReceiptCreateUpdateDto dto)
        {
            var userId = 1; // TODO: from claims

            try
            {
                var id = await _service.CreateAsync(dto, userId);
                return Ok(new { isSuccess = true, id });
            }
            catch (InvalidOperationException ex)
            {
                // Period locked / validation issues
                return BadRequest(new { isSuccess = false, message = ex.Message });
            }
        }

        // PUT api/ArReceipt/5
        [HttpPut("update/{id:int}")]
        public async Task<IActionResult> Update(int id, [FromBody] ArReceiptCreateUpdateDto dto)
        {
            dto.Id = id;
            var userId = 1;

            try
            {
                await _service.UpdateAsync(dto, userId);
                return Ok(new { isSuccess = true });
            }
            catch (InvalidOperationException ex)
            {
                return BadRequest(new { isSuccess = false, message = ex.Message });
            }
        }

        // DELETE api/ArReceipt/5
        [HttpDelete("delete/{id:int}")]
        public async Task<IActionResult> Delete(int id)
        {
            var userId = 1;
            await _service.DeleteAsync(id, userId);
            return Ok(new { isSuccess = true });
        }
    }
}
