using FinanceApi.Data;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using FinanceApi.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace FinanceApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CustomerMasterController : ControllerBase
    {
        private readonly ICustomerMasterService _customerMasterService;
        private readonly IKycService _kycService;


        public CustomerMasterController(ICustomerMasterService customerMasterService, IKycService kycService)
        {
            _customerMasterService = customerMasterService;
            _kycService = kycService;
        }


        [HttpGet("GetAllCustomerMaster")]
        public async Task<IActionResult> GetAllCustomerMaster()
        {
            var list = await _customerMasterService.GetAllAsync();
            ResponseResult data = new ResponseResult(true, "Customer Master retrieved successfully", list);
            return Ok(data);
        }


        [HttpGet("GetCustomerMasterById/{id}")]
        public async Task<IActionResult> GetCustomerMasterById(int id)
        {
            var approvalLevel = await _customerMasterService.GetById(id);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "Customer Master not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }




        [HttpPost("CreateCustomerMaster")]
        public async Task<ActionResult> CreateCustomerMaster(CustomerMaster customerMaster)
        {

            customerMaster.CreatedDate = DateTime.Now;
                var id = await _customerMasterService.CreateAsync(customerMaster);
                ResponseResult data = new ResponseResult(true, "Customer Master created successfully", id);
                return Ok(data);
           

        }
        [HttpPut("UpdateCustomerMasterById")]
        [Consumes("multipart/form-data")]
        public async Task<IActionResult> UpdateCustomer([FromForm] UpdateCustomerRequest req)
        {
            try
            {
                await _customerMasterService.UpdateAsync(req);
                return Ok(new ResponseResult(true, "Customer Master updated successfully.", null));
            }
            catch (Exception ex)
            {
                return StatusCode(500, new ResponseResult(false, ex.Message, null));
            }
        }



        [HttpPost("CreateCustomerWithKYC")]
        public async Task<IActionResult> CreateCustomerWithKYC([FromForm] CreateCustomerWithKycRequest req)
        {
            try
            {
                // ✅ 1. Ensure folder exists
                var root = Path.Combine(Directory.GetCurrentDirectory(), "wwwroot", "uploads", "kyc");
                if (!Directory.Exists(root)) Directory.CreateDirectory(root);

                // ✅ 2. Helper to save file
                string? SaveFile(IFormFile? file)
                {
                    if (file == null) return null;
                    var fileName = $"{Guid.NewGuid()}_{Path.GetFileName(file.FileName)}";
                    var path = Path.Combine(root, fileName);
                    using (var stream = new FileStream(path, FileMode.Create))
                        file.CopyTo(stream);
                    return $"/uploads/kyc/{fileName}"; // relative URL
                }

                // ✅ 3. Save all files
                var dlPath = SaveFile(req.DrivingLicence);
                var utilPath = SaveFile(req.UtilityBill);
                var bsPath = SaveFile(req.BankStatement);
                var acraPath = SaveFile(req.Acra);

                // ✅ 4. Create KYC entry first
                var kyc = new KYC
                {
                    DLImage = dlPath,
                    UtilityBillImage = utilPath,
                    BSImage = bsPath,
                    ACRAImage = acraPath,

                    DLImageName = req.DrivingLicence?.FileName,
                    UtilityBillImageName = req.UtilityBill?.FileName,
                    BSImageName = req.BankStatement?.FileName,
                    ACRAImageName = req.Acra?.FileName,
                    ApprovedBy =   req.ApprovedBy,
                    IsApproved = req.IsApproved,
                    IsActive = true,
                    CreatedDate = DateTime.Now,
                    CreatedBy = req.CreatedBy
                };

                // this returns new KYC Id
                var kycId = await _kycService.CreateAsync(kyc);

                // ✅ 5. Create Customer entry — pass that KYC Id here
                var customer = new CustomerMaster
                {
                    CustomerName = req.CustomerName,
                    CountryId = req.CountryId,
                    LocationId = req.LocationId,
                    ContactNumber = req.ContactNumber,
                    PointOfContactPerson = req.PointOfContactPerson,
                    Email = req.Email,
                    CustomerGroupId = req.CustomerGroupId,
                    BudgetLineId = req.BudgetLineId,
                    PaymentTermId = req.PaymentTermId,
                    CreditAmount = req.CreditAmount,
                    KycId = kycId, // 👈 here we pass the returned KYC Id
                    CreatedDate = DateTime.Now,
                    CreatedBy = req.CreatedBy,
                    UpdatedDate = DateTime.Now,
                    UpdatedBy = req.CreatedBy,
                    IsActive = true
                };

                var customerId = await _customerMasterService.CreateAsync(customer);

                return Ok(new
                {
                    isSuccess = true,
                    message = "Customer & KYC created successfully!",
                    kycId,
                    customerId
                });
            }
            catch (Exception ex)
            {
                return BadRequest(new { isSuccess = false, message = ex.Message });
            }
        }


        [HttpGet("GetAllCustomerDetails")]
        public async Task<IActionResult> GetAllCustomerDetails()
        {
            var list = await _customerMasterService.GetAllCustomerDetails();
            ResponseResult data = new ResponseResult(true, "Customer Master retrieved successfully", list);
            return Ok(data);
        }


        [HttpGet("EditLoadforCustomerbyId/{id}")]
        public async Task<IActionResult> EditCustomerbyId(int id)
        {
            var approvalLevel = await _customerMasterService.EditLoadforCustomerbyId(id);
            if (approvalLevel == null)
            {
                ResponseResult data1 = new ResponseResult(false, "Customer Master not found", null);
                return Ok(data1);
            }
            ResponseResult data = new ResponseResult(true, "Success", approvalLevel);
            return Ok(data);
        }


        [HttpDelete("Deactivate/{customerId:int}/{kycId:int?}")]
        public async Task<IActionResult> Deactivate(int customerId, int? kycId)
        {
            var ok = await _customerMasterService.DeactivateAsync(customerId, kycId);
            return ok
              ? Ok(new ResponseResult(true, "Customer (and KYC if provided) deactivated.", null))
              : NotFound(new ResponseResult(false, "Customer not found or already inactive.", null));
        }

    }
}
