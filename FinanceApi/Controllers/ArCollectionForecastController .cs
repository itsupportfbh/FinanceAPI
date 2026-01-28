using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using FinanceApi.Data;              // for ResponseResult
using FinanceApi.Interfaces;        // for IArCollectionForecastService

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ArCollectionForecastController : ControllerBase
    {
        private readonly IArCollectionForecastService _service;

        public ArCollectionForecastController(IArCollectionForecastService service)
        {
            _service = service;
        }

        // GET: api/ArCollectionForecast/summary
        // Optional query params (?fromDate=2025-11-01&toDate=2025-11-30)
        [HttpGet("summary")]
        public async Task<IActionResult> GetSummary(
            [FromQuery] DateTime? fromDate,
            [FromQuery] DateTime? toDate)
        {
            var data = await _service.GetSummaryAsync(fromDate, toDate);
            return Ok(new ResponseResult(true, "Success", data));
        }

        // GET: api/ArCollectionForecast/detail/1
        // Optional query params (?fromDate=2025-11-01&toDate=2025-11-30)
        [HttpGet("detail/{customerId:int}")]
        public async Task<IActionResult> GetDetail(
            int customerId,
            [FromQuery] DateTime? fromDate,
            [FromQuery] DateTime? toDate)
        {
            var data = await _service.GetDetailAsync(customerId, fromDate, toDate);
            return Ok(new ResponseResult(true, "Success", data));
        }
    }
}
