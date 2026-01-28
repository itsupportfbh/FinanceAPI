using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class GeneralLedgerController : ControllerBase
    {
        private readonly IGeneralLedgerService _generalLedgerService;

        public GeneralLedgerController(IGeneralLedgerService generalLedgerService)
        {
            _generalLedgerService = generalLedgerService;
        }


        [HttpGet("GetGeneralLedger")]

        public async Task<IActionResult> GetGeneralLedger()
        {
            var list = await _generalLedgerService.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "General Ledger retrieved successfully", list);
            return Ok(data);
        }
    }
}
