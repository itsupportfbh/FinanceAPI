using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel;

namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class TaxCodeController : ControllerBase
    {
        private readonly ITaxCodeService _service;

        public TaxCodeController(ITaxCodeService service)
        {
            _service = service;
        }
        [HttpGet("getAll")]
        public async Task<IActionResult> GetAll()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Success", list);
            return Ok(data);
        }



        [HttpPost("insert")]
        public async Task<ActionResult> Create(TaxCode taxCode)
        {
            var taxCodeName = await _service.GetByName(taxCode.Name);
            if (taxCodeName == null)
            {
                var id = await _service.CreateAsync(taxCode);
                ResponseResult data = new ResponseResult(true, "TaxCode created sucessfully", id);
                return Ok(data);
            }
            else
            {
                ResponseResult data = new ResponseResult(false, "TaxCode  Already Exist.", null);
                return Ok(data);
            }

        }


        [HttpGet("get/{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var licenseObj = await _service.GetById(id);
            ResponseResult data = new ResponseResult(true, "Success", licenseObj);
            return Ok(data);
        }


        [HttpPut("update")]
        public async Task<IActionResult> Update(TaxCode taxCode)
        {
            var isTaxCodeExist = await _service.GetByName(taxCode.Name);
            if (isTaxCodeExist != null && isTaxCodeExist.Id!= taxCode.Id) 
            {
                ResponseResult responseResult = new ResponseResult(false, "TaxCode Already Exists", taxCode);
                return Ok(responseResult);

            }



            await _service.UpdateAsync(taxCode);
            ResponseResult data = new ResponseResult(true, "TaxCode updated successfully.", null);
            return Ok(data);

        }



        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteLicense(id);
            ResponseResult data = new ResponseResult(true, "TaxCode Deleted sucessfully", null);
            return Ok(data);
        }


        [HttpGet("GetTaxCodeByName/{name}")]
        public async Task<IActionResult> GetByName(string name)
        {
            var approvalLevel = await _service.GetByName(name);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "TaxCode level not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }
    }
}