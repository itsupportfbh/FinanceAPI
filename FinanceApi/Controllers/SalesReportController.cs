using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class SalesReportController : ControllerBase
    {
        private readonly ISalesReportService _salesReportService;

        public SalesReportController(ISalesReportService salesReportService)
        {
            _salesReportService = salesReportService;
        }




        [HttpGet("GetSalesByItemAsync")]
        public async Task<IActionResult> GetSalesByItemAsync()
        {
            var list = await _salesReportService.GetSalesByItemAsync();
            ResponseResult data = new ResponseResult(true, "SalesByItem  retrieved successfully", list);
            return Ok(data);
        }


        [HttpGet("GetSalesMarginAsync")]
        public async Task<IActionResult> GetSalesMarginAsync()
        {
            var list = await _salesReportService.GetSalesMarginAsync();
            ResponseResult data = new ResponseResult(true, "SalesMargin  retrieved successfully", list);
            return Ok(data);
        }

    }
}
