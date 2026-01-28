using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CurrencyController : ControllerBase
    {
        private readonly ICurrencyService _service;

        public CurrencyController(ICurrencyService service)
        {
            _service = service;
        }


        [HttpGet("GetCurrencies")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Currency retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("GetCurrencyById/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var approvalLevel = await _service.GetById(id);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "Currency not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }




        [HttpPost("CreateCurrency")]
        public async Task<ActionResult> Create(Currency currencyDTO)
        {
            //currencyDTO.CreatedDate = DateTime.Now;
            //var id = await _service.CreateAsync(currencyDTO);
            //ResponseResult data = new ResponseResult(true, "Currency created successfully", id);
            //return Ok(data);

            var currencyName = await _service.GetByName(currencyDTO.CurrencyName);
            if (currencyName == null)
            {
                currencyDTO.CreatedDate = DateTime.Now;
                var id = await _service.CreateAsync(currencyDTO);
                ResponseResult data = new ResponseResult(true, "Currency  created successfully", id);
                return Ok(data);
            }
            else
            {
                ResponseResult data = new ResponseResult(false, "Currency  Already Exist.", null);
                return Ok(data);
            }

        }

        [HttpPut("UpdateCurrencyById/{id}")]
        public async Task<IActionResult> Update(Currency currencyDTO)
        {
            //await _service.UpdateAsync(currencyDTO);
            //ResponseResult data = new ResponseResult(true, "Currency updated successfully.", null);
            //return Ok(data);

            var exists = await _service.NameExistsAsync(currencyDTO.CurrencyName, currencyDTO.Id);
            if (exists)
            {
                // You can also return Conflict(...) if you prefer a 409 status
                var dup = new ResponseResult(false, "Currency already exists.", null);
                return Ok(dup);
            }

            await _service.UpdateAsync(currencyDTO);
            var ok = new ResponseResult(true, "Currency  updated successfully.", null);
            return Ok(ok);
        }

        [HttpDelete("DeleteCurrencyById/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "Currency Deleted sucessfully", null);
            return Ok(data);
        }
    }
}
