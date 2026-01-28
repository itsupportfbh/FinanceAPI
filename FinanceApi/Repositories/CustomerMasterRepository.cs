using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Data;

namespace FinanceApi.Repositories
{
    public class CustomerMasterRepository : DynamicRepository, ICustomerMasterRepository
    {
        public CustomerMasterRepository(IDbConnectionFactory connectionFactory)
       : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<CustomerMasterDTO>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Customer";

            return await Connection.QueryAsync<CustomerMasterDTO>(query);
        }


        public async Task<CustomerMasterDTO> GetByIdAsync(int id)
        {

            const string query = "SELECT * FROM Customer WHERE Id = @Id";

            return await Connection.QuerySingleAsync<CustomerMasterDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(CustomerMaster customerMaster)
        {
            // 1) Get parent COA row (budget line selected in UI)
            const string parentSql = @"
SELECT TOP 1 Id,
             HeadCode,
             HeadName,
             HeadLevel,
             HeadType
FROM ChartOfAccount
WHERE Id = @Id
  AND IsActive = 1;";

            var parent = await Connection.QuerySingleAsync<dynamic>(
                parentSql,
                new { Id = customerMaster.BudgetLineId }
            );

            int parentHeadCode = parent.HeadCode;
            string parentName = parent.HeadName;
            int parentHeadLevel = parent.HeadLevel;
            string parentType = parent.HeadType;   // e.g. 'A' / 'L' / 'I' / 'E'

            // 2) Generate next HeadCode under this parent
            const string nextCodeSql = @"
SELECT ISNULL(MAX(HeadCode), 0) + 1
FROM ChartOfAccount
WHERE ParentHead = @ParentHead;";

            int newHeadCode = await Connection.ExecuteScalarAsync<int>(
                nextCodeSql,
                new { ParentHead = parentHeadCode }
            );

            // 3) Insert new ChartOfAccount row for this customer
            var now = DateTime.UtcNow;

            var coaParams = new
            {
                HeadCode = newHeadCode,
                HeadLevel = parentHeadLevel + 1,
                HeadName = customerMaster.CustomerName,
                HeadType = parentType,
                HeadCodeName = $"{newHeadCode} - {customerMaster.CustomerName}",
                IsGl = true,
                IsTransaction = true,
                ParentHead = parentHeadCode,
                PHeadName = parentName,
                CreatedBy = customerMaster.CreatedBy ,
                CreatedDate = now,
                UpdatedBy = customerMaster.UpdatedBy,
                UpdatedDate = now,
                IsActive = true
            };

            const string coaInsertSql = @"
INSERT INTO ChartOfAccount
(HeadCode, HeadLevel, HeadName, HeadType, HeadCodeName,
 IsGl, IsTransaction, ParentHead, PHeadName,
 CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive)
OUTPUT INSERTED.Id
VALUES
(@HeadCode, @HeadLevel, @HeadName, @HeadType, @HeadCodeName,
 @IsGl, @IsTransaction, @ParentHead, @PHeadName,
 @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive);";

            int newCoaId = await Connection.QueryFirstAsync<int>(
                coaInsertSql,
                coaParams
            );

            // 4) Use that COA Id as the customer's BudgetLineId
            customerMaster.BudgetLineId = newCoaId;

            // 5) Insert Customer
            const string customerInsertSql = @"
INSERT INTO Customer (
    CustomerName,
    CountryId,
    LocationId,
    ContactNumber,
    PointOfContactPerson,
    Email,
    CustomerGroupId,
    BudgetLineId,
    PaymentTermId,
    CreditAmount,
    KycId,
    CreatedDate,
    CreatedBy,
    UpdatedDate,
    UpdatedBy,
    IsActive
)
OUTPUT INSERTED.Id
VALUES (
    @CustomerName,
    @CountryId,
    @LocationId,
    @ContactNumber,
    @PointOfContactPerson,
    @Email,
    @CustomerGroupId,
    @BudgetLineId,
    @PaymentTermId,
    @CreditAmount,
    @KycId,
    @CreatedDate,
    @CreatedBy,
    @UpdatedDate,
    @UpdatedBy,
    @IsActive
);";

            int newCustomerId = await Connection.QueryFirstAsync<int>(
                customerInsertSql,
                customerMaster
            );

            return newCustomerId;
        }


        public async Task<IEnumerable<CustomerList>> GetAllCustomerDetails()
        {
            const string query = @"
              select 
c.Id as CustomerId,c.CustomerName,c.ContactNumber,c.PointOfContactPerson,c.Email,c.CreditAmount,
pt.PaymentTermsName,
cg.Name as CustomerGroupName,
cn.CountryName,
l.Name as LocationName,
kyc.isApproved,kyc.Id as KycId
from Customer as c
inner join Kyc on kyc.Id = c.KycId
inner join CustomerGroups as cg on cg.Id = c.CustomerGroupId
inner join ChartOfAccount as ca on ca.Id = c.BudgetLineId
inner join PaymentTerms as pt on pt.Id = c.PaymentTermId
inner join Location as l on l.Id = c.LocationId
inner join Country as cn on cn.Id = c.CountryId
where c.IsActive = 1 and kyc.isActive =1";

            return await Connection.QueryAsync<CustomerList>(query);
        }



        public async Task<CustomerList> EditLoadforCustomerbyId(int id)
        {

            const string query = @"
              select 
c.Id as CustomerId,c.CustomerName,c.ContactNumber,c.PointOfContactPerson,c.Email,c.CreditAmount,c.ContactNumber,
c.CountryId,c.LocationId,c.CustomerGroupId,c.BudgetLineId,c.PaymentTermId,
pt.PaymentTermsName,
cg.Name as CustomerGroupName,
cn.CountryName,
l.Name as LocationName,
kyc.isApproved,kyc.ApprovedBy,kyc.DLImage,kyc.BSImage,kyc.UtilityBillImage,kyc.ACRAImage,kyc.Id as KycId
from Customer as c
inner join Kyc on kyc.Id = c.KycId
inner join CustomerGroups as cg on cg.Id = c.CustomerGroupId
inner join ChartOfAccount  as ca on ca.Id = c.BudgetLineId
inner join PaymentTerms as pt on pt.Id = c.PaymentTermId
inner join Location as l on l.Id = c.LocationId
inner join Country as cn on cn.Id = c.CountryId
where c.Id =@Id";

            return await Connection.QuerySingleAsync<CustomerList>(query, new { Id = id });
        }


        public async Task<bool> UpdateAsync(UpdateCustomerRequest req)
        {
            if (req.CustomerId <= 0) return false;

            const string updateCustomer = @"
UPDATE Customer SET 
    CustomerName = @CustomerName,
    CountryId = @CountryId,
    LocationId = @LocationId,
    ContactNumber = @ContactNumber,
    PointOfContactPerson = @PointOfContactPerson,
    Email = @Email,
    CustomerGroupId = @CustomerGroupId,
    BudgetLineId = @BudgetLineId,
    PaymentTermId = @PaymentTermId,
    CreditAmount = @CreditAmount
WHERE Id = @CustomerId;";

            await Connection.ExecuteAsync(updateCustomer, req);

            // ❌ REMOVE this:
            // if (req.IsApproved) return true;

            // Prepare file storage
            string root = Path.Combine(Directory.GetCurrentDirectory(), "wwwroot", "uploads", "kyc");
            if (!Directory.Exists(root)) Directory.CreateDirectory(root);

            async Task<string?> SaveFileAsync(IFormFile? file)
            {
                if (file == null || file.Length == 0) return null;
                var safeName = Path.GetFileName(file.FileName);
                var fileName = $"{Guid.NewGuid()}_{safeName}";
                var path = Path.Combine(root, fileName);
                using (var stream = new FileStream(path, FileMode.Create))
                    await file.CopyToAsync(stream);
                return $"/uploads/kyc/{fileName}";
            }

            // Save any uploaded files (only writes if a file was actually sent)
            var dlPath = await SaveFileAsync(req.DrivingLicence);
            var utilPath = await SaveFileAsync(req.UtilityBill);
            var bsPath = await SaveFileAsync(req.BankStatement);
            var acraPath = await SaveFileAsync(req.Acra);

            // Decide if we need to touch KYC at all
            bool HasAnyKycPayload() =>
                dlPath != null || utilPath != null || bsPath != null || acraPath != null ||
                !string.IsNullOrWhiteSpace(req.DlImageName) ||
                !string.IsNullOrWhiteSpace(req.UtilityBillImageName) ||
                !string.IsNullOrWhiteSpace(req.BsImageName) ||
                !string.IsNullOrWhiteSpace(req.AcraImageName) ||
                req.ApprovedBy.HasValue || req.IsApproved;

            if (!HasAnyKycPayload())
                return true; // customer updated; nothing for KYC

            if (req.KycId is > 0)
            {
                const string updateKyc = @"
UPDATE Kyc SET
    DLImage = ISNULL(@DLImage, DLImage),
    DLImageName = ISNULL(@DLImageName, DLImageName),
    UtilityBillImage = ISNULL(@UtilityBillImage, UtilityBillImage),
    UtilityBillImageName = ISNULL(@UtilityBillImageName, UtilityBillImageName),
    BSImage = ISNULL(@BSImage, BSImage),
    BSImageName = ISNULL(@BSImageName, BSImageName),
    ACRAImage = ISNULL(@ACRAImage, ACRAImage),
    ACRAImageName = ISNULL(@ACRAImageName, ACRAImageName),
    ApprovedBy = ISNULL(@ApprovedBy, ApprovedBy),
    IsApproved = ISNULL(@IsApproved, IsApproved)
WHERE Id = @KycId;";

                await Connection.ExecuteAsync(updateKyc, new
                {
                    req.KycId,
                    DLImage = dlPath,
                    DLImageName = req.DlImageName,
                    UtilityBillImage = utilPath,
                    UtilityBillImageName = req.UtilityBillImageName,
                    BSImage = bsPath,
                    BSImageName = req.BsImageName,
                    ACRAImage = acraPath,
                    ACRAImageName = req.AcraImageName,
                    req.ApprovedBy,
                    req.IsApproved
                });
            }
            else
            {
                const string insertKyc = @"
INSERT INTO Kyc (DLImage, DLImageName, UtilityBillImage, UtilityBillImageName,
                 BSImage, BSImageName, ACRAImage, ACRAImageName, ApprovedBy, IsApproved)
VALUES (@DLImage, @DLImageName, @UtilityBillImage, @UtilityBillImageName,
        @BSImage, @BSImageName, @ACRAImage, @ACRAImageName, @ApprovedBy, @IsApproved);
SELECT CAST(SCOPE_IDENTITY() AS INT);";

                int newKycId = await Connection.ExecuteScalarAsync<int>(insertKyc, new
                {
                    DLImage = dlPath,
                    DLImageName = req.DlImageName,
                    UtilityBillImage = utilPath,
                    UtilityBillImageName = req.UtilityBillImageName,
                    BSImage = bsPath,
                    BSImageName = req.BsImageName,
                    ACRAImage = acraPath,
                    ACRAImageName = req.AcraImageName,
                    req.ApprovedBy,
                    req.IsApproved
                });

                await Connection.ExecuteAsync(
                    "UPDATE Customer SET KycId = @newKycId WHERE Id = @CustomerId;",
                    new { newKycId, req.CustomerId });
            }

            return true;
        }

        public async Task<bool> DeactivateAsync(int customerId, int? kycId)
        {
            // 1) Deactivate Customer
            const string deactivateCustomer = @"
        UPDATE Customer
        SET IsActive = 0,
            UpdatedDate = SYSUTCDATETIME()
        WHERE Id = @customerId AND IsActive = 1;
    ";

            var rows = await Connection.ExecuteAsync(deactivateCustomer, new { customerId });

            // If no rows → customer not found or already inactive
            if (rows == 0)
                return false;

            // 2) If KYC ID is provided → deactivate KYC
            if (kycId.HasValue)
            {
                const string deactivateKyc = @"
            UPDATE Kyc
            SET IsActive = 0,
                UpdatedDate = SYSUTCDATETIME()
            WHERE Id = @kycId AND  IsActive = 1;
        ";

                await Connection.ExecuteAsync(deactivateKyc, new { customerId, kycId });
            }

            return true;
        }






    }
}
