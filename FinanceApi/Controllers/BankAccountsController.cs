// Controllers/BankAccountsController.cs
using FinanceApi.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class BankAccountsController : ControllerBase
    {
        private readonly IBankAccountRepository _bankRepo;

        public BankAccountsController(IBankAccountRepository bankRepo)
        {
            _bankRepo = bankRepo;
        }

        [HttpGet]
        public async Task<IActionResult> GetBankAccounts()
        {
            var data = await _bankRepo.GetBankAccountsAsync();
            return Ok(new
            {
                isSuccess = true,
                message = "Success",
                data
            });
        }
    }
}
