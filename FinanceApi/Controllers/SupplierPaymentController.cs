using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SupplierPaymentController : ControllerBase
    {
        private readonly ISupplierPaymentService _service;

        public SupplierPaymentController(ISupplierPaymentService service)
        {
            _service = service;
        }

        [HttpGet("GetAll")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            var data = new ResponseResult(true, "Supplier payments retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("GetBySupplier/{supplierId}")]
        public async Task<IActionResult> GetBySupplier(int supplierId)
        {
            var list = await _service.GetBySupplierAsync(supplierId);
            var data = new ResponseResult(true, "Supplier payments by supplier retrieved", list);
            return Ok(data);
        }

        [HttpPost("Create")]
        public async Task<IActionResult> Create([FromBody] SupplierPaymentCreateDTO dto)
        {
            if (dto == null || dto.Amount <= 0)
            {
                return BadRequest(new ResponseResult(false, "Invalid payment data", null));
            }

            var ok = await _service.CreateAsync(dto);
            if (!ok)
            {
                return BadRequest(new ResponseResult(false, "Failed to create payment", null));
            }

            return Ok(new ResponseResult(true, "Payment created successfully", null));
        }
    }
}
