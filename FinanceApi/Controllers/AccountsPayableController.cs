// Controllers/AccountsPayableController.cs
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Security.Claims;
using System.Threading.Tasks;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/finance/ap")]
    public class AccountsPayableController : ControllerBase
    {
        private readonly IAccountsPayableService _service;

        public AccountsPayableController(IAccountsPayableService service)
        {
            _service = service;
        }

        // ---------- INVOICES ----------
        [HttpGet("invoices")]
        public async Task<IActionResult> GetApInvoices()
        {
            var rows = await _service.GetApInvoicesAsync();
            return Ok(new { isSuccess = true, data = rows });
        }

        [HttpGet("invoices/supplier/{supplierId:int}")]
        public async Task<IActionResult> GetApInvoicesBySupplier(int supplierId)
        {
            var rows = await _service.GetApInvoicesBySupplierAsync(supplierId);
            return Ok(new { isSuccess = true, data = rows });
        }

        // ---------- PAYMENTS ----------
        [HttpGet("payments")]
        public async Task<IActionResult> GetPayments()
        {
            var rows = await _service.GetPaymentsAsync();
            return Ok(new { isSuccess = true, data = rows });
        }

        [HttpPost("payments/create")]
        public async Task<IActionResult> CreatePayment([FromBody] ApPaymentCreateDto dto)
        {
            if (dto == null)
                return BadRequest(new { isSuccess = false, message = "Payload missing" });

            try
            {
                var userId = 1; // TODO: from JWT
                var id = await _service.CreatePaymentAsync(dto, userId);

                return Ok(new
                {
                    isSuccess = true,
                    message = "Payment posted",
                    data = new { id }
                });
            }
            catch (InvalidOperationException ex)
            {
                // Period locked / no period
                return BadRequest(new { isSuccess = false, message = ex.Message });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { isSuccess = false, message = ex.Message });
            }
        }

        // ---------- 3-WAY MATCH ----------
        [HttpGet("match")]
        public async Task<IActionResult> GetMatchList()
        {
            var rows = await _service.GetMatchListAsync();
            return Ok(new { isSuccess = true, data = rows });
        }
        [HttpGet("bankaccount")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            return Ok(new { isSuccess = true, data = list });
        }

        // GET: /finance/bank-accounts/{id}
        [HttpGet("bankaccount/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var item = await _service.GetByIdAsync(id);
            if (item == null)
                return NotFound(new { isSuccess = false, message = "Bank account not found" });

            return Ok(new { isSuccess = true, data = item });
        }
        [HttpPost("update-bank-balance")]
        public async Task<IActionResult> UpdateBankBalance([FromBody] BankBalanceUpdateDto dto)
        {
            var count = await _service.UpdateBankBalance(dto.BankHeadId, dto.NewBalance);

            if (count <= 0)
                return Ok(new { isSuccess = false, message = "No row updated" });

            return Ok(new { isSuccess = true, message = "Bank balance updated" });
        }
        [HttpGet("supplier-advances")]
        public async Task<IActionResult> GetSupplierAdvances()
        {
            var data = await _service.GetSupplierAdvancesAsync();

            // If you usually wrap: return Ok(new { data });
            return Ok(new { data });
        }
        [HttpPost("createsupplier-advance")]
       
        public async Task<IActionResult> CreateSupplierAdvance([FromBody] ApSupplierAdvanceCreateRequest req)
        {
            if (req == null)
                return BadRequest(new { isSuccess = false, message = "Invalid request" });

            try
            {
                int userId = 1;
                var userIdStr = User.FindFirstValue(ClaimTypes.NameIdentifier);
                if (!string.IsNullOrEmpty(userIdStr))
                    int.TryParse(userIdStr, out userId);

                var id = await _service.CreateSupplierAdvanceAsync(userId, req);

                return Ok(new
                {
                    isSuccess = true,
                    id
                });
            }
            catch (Exception ex)
            {
                return BadRequest(new
                {
                    isSuccess = false,
                    message = ex.Message
                });
            }
        }

        // ============= GET SUPPLIER ADVANCES (BALANCE) =============
        [HttpGet("supplier-advances/{supplierId:int}")]
        public async Task<IActionResult> GetSupplierAdvances(int supplierId)
        {
            if (supplierId <= 0)
                return BadRequest(new { isSuccess = false, message = "Invalid supplier" });

            var rows = await _service.GetSupplierAdvancesAsync(supplierId);

            return Ok(new
            {
                isSuccess = true,
                data = rows
            });
        }
        // Controllers/AccountsPayableController.cs

        [HttpGet("getsupplier-advances")]
        public async Task<IActionResult> GetSupplierAdvancesList()
        {
            var rows = await _service.GetSupplierAdvancesListAsync();

            return Ok(new
            {
                isSuccess = true,
                data = rows
            });
        }
        [HttpGet("list")]
        public async Task<IActionResult> GetList()
        {
            var result = await _service.GetAdvanceListAsync();
            return Ok(new { data = result });
        }
    }
}
