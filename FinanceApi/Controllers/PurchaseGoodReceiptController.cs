using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PurchaseGoodReceiptController : ControllerBase
    {
        private readonly IPurchaseGoodReceiptService _service;

        public PurchaseGoodReceiptController(IPurchaseGoodReceiptService service)
        {
            _service = service;
        }


        [HttpGet("GetAllGRN")]
        public async Task<IActionResult> GetAllGRN()
        {
            var list = await _service.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "PurchaseGoodReceipt retrieved successfully", list);
            return Ok(data);
        }

        [HttpGet("getGRNbyId/{id}")]
        public async Task<IActionResult> getGRNbyId(int id)
        {
            var approvalLevel = await _service.GetById(id);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "PurchaseGoodReceipt not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }

        [HttpPost("insertGRN")]
        public async Task<ActionResult> insertGRN(
     [FromBody] PurchaseGoodReceiptItems purchaseGoodReceiptItems)
        {
            try
            {
                var id = await _service.CreateAsync(purchaseGoodReceiptItems);
                ResponseResult data = new ResponseResult(true, "PurchaseGoodReceipt created successfully", id);
                return Ok(data);
            }
            catch (InvalidOperationException ex)
            {
                // Period locked / validation problems
                ResponseResult data = new ResponseResult(false, ex.Message, null);
                return Ok(data);
            }
        }

        [HttpPut("update")]
        public async Task<IActionResult> Update([FromBody] PurchaseGoodReceiptItems purchaseGoodReceipt)
        {
            try
            {
                await _service.UpdateAsync(purchaseGoodReceipt);
                ResponseResult data = new ResponseResult(true, "FlagIssues updated successfully.", null);
                return Ok(data);
            }
            catch (InvalidOperationException ex)
            {
                ResponseResult data = new ResponseResult(false, ex.Message, null);
                return Ok(data);
            }
        }




        [HttpGet("GetAllGRNDetails")]
        public async Task<IActionResult> GetAllGRNDetails()
        {
            var list = await _service.GetAllGRNDetailsAsync();
            ResponseResult data = new ResponseResult(true, "PurchaseGoodReceipt retrieved successfully", list);
            return Ok(data);
        }



        //[HttpPut("update")]
        //public async Task<IActionResult> Update(PurchaseGoodReceiptItems purchaseGoodReceipt)
        //{
        //    await _service.UpdateAsync(purchaseGoodReceipt);
        //    ResponseResult data = new ResponseResult(true, "FlagIssues updated successfully.", null);
        //    return Ok(data);
        //}


        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            await _service.DeleteAsync(id);
            ResponseResult data = new ResponseResult(true, "PurchaseGoodReceipt Deleted sucessfully", null);
            return Ok(data);
        }
        [HttpGet("GetAllGRNByPoId")]
        public async Task<IActionResult> GetAllGRNByPoId()
        {
            var list = await _service.GetAllGRNByPoId();
            ResponseResult data = new ResponseResult(true, "PurchaseGoodReceipt retrieved successfully", list);
            return Ok(data);
        }


        [HttpPost("apply-grn-update-salesorder")]
        public async Task<IActionResult> ApplyGrnAndUpdateSalesOrder([FromBody] ApplyGrnAndSalesOrderRequest req)
        {
            try
            {
                var result = await _service.ApplyGrnAndUpdateSalesOrderAsync(req);

                // result.Message already says either updated or no SO
                return Ok(new
                {
                    message = result.Message,
                    updatedSalesOrderLines = result.UpdatedSalesOrderLines,
                    itemId = result.ItemId
                });
            }
            catch (Exception ex)
            {
                // only real errors come here: itemcode not found, sql error, etc.
                return StatusCode(500, new { message = "Error applying GRN update", error = ex.Message });
            }
        }

    }
}
