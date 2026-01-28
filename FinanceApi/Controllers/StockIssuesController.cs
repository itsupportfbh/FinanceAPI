using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class StockIssuesController : ControllerBase
    {
        private readonly IStockIssueServices _service;
        public StockIssuesController(IStockIssueServices service)
        {
            _service = service;
        }


        [HttpGet("GetAllStockissue")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }



        [HttpPost("createStockissue")]
        public async Task<ActionResult> Create(StockIssues StockissuesDTO)
        {

            var id = await _service.CreateAsync(StockissuesDTO);
            ResponseResult data = new ResponseResult(true, "Stockissues created sucessfully", id);
            return Ok(data);

        }



        [HttpGet("getStockissueById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPut("updateStockissueById/{id}")]
        public async Task<IActionResult> Update(StockIssues StockissuesDTO)
        {
            await _service.UpdateAsync(StockissuesDTO);
            ResponseResult data = new ResponseResult(true, "Stockissues updated successfully.", null);
            return Ok(data);
        }



        [HttpDelete("deleteStockissueById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "Stockissues Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
