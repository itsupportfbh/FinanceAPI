using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class BankRepository : DynamicRepository,IBankRepository
    {
        public BankRepository(IDbConnectionFactory connectionFactory)
: base(connectionFactory)
        {
        }


        public async Task<IEnumerable<BankDto>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Bank
where isActive = 1
                ORDER BY ID
";

            return await Connection.QueryAsync<BankDto>(query);
        }


        public async Task<BankDto> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM Bank WHERE Id = @Id";

            return await Connection.QuerySingleAsync<BankDto>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Bank bankDto)
        {
            // 1) Get parent COA row (the group under which the bank account will sit)
            const string parentSql = @"
SELECT TOP 1 Id, HeadCode, HeadName, HeadLevel, HeadType
FROM ChartOfAccount
WHERE Id = @Id AND IsActive = 1;";

            var parent = await Connection.QuerySingleAsync<dynamic>(
                parentSql,
                new { Id = bankDto.BudgetLineId }
            );

            int parentHeadCode = parent.HeadCode;
            string parentName = parent.HeadName;
            int parentHeadLevel = parent.HeadLevel;
            string parentType = parent.HeadType;   // usually 'A' for Asset

            // 2) Generate next HeadCode under this parent
            const string nextCodeSql = @"
SELECT ISNULL(MAX(HeadCode), 0) + 1
FROM ChartOfAccount
WHERE ParentHead = @ParentHead;";

            int newHeadCode = await Connection.ExecuteScalarAsync<int>(
                nextCodeSql,
                new { ParentHead = parentHeadCode }
            );

            // 3) Insert new ChartOfAccount row for this bank
            var now = DateTime.UtcNow;

            var coaParams = new
            {
                HeadCode = newHeadCode,
                HeadLevel = parentHeadLevel + 1,
                HeadName = bankDto.BankName,
                HeadType = parentType,
                HeadCodeName = $"{newHeadCode} - {bankDto.BankName}",
                IsGl = true,
                IsTransaction = true,
                ParentHead = parentHeadCode,
                PHeadName = parentName,
                CreatedBy = bankDto.CreatedBy,
                CreatedDate = now,
                UpdatedBy = bankDto.UpdatedBy,
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

            // 4) Use that COA Id as the bank's BudgetLineId
            bankDto.BudgetLineId = newCoaId;

            // 5) Insert Bank
            const string bankInsertSql = @"
INSERT INTO Bank 
(
    BankName,
    AccountHolderName,
    AccountNo,
    AccountType,
    Branch,
    IFSC,
    Routing,
    CurrencyId,
    CountryId,
    PrimaryContact,
    Email,
    ContactNo,
    Address,
    IsActive,
    BudgetLineId
)
OUTPUT INSERTED.Id
VALUES
(
    @BankName,
    @AccountHolderName,
    @AccountNo,
    @AccountType,
    @Branch,
    @IFSC,
    @Routing,
    @CurrencyId,
    @CountryId,
    @PrimaryContact,
    @Email,
    @ContactNo,
    @Address,
    @IsActive,
    @BudgetLineId
);";

            int newBankId = await Connection.QueryFirstAsync<int>(
                bankInsertSql,
                bankDto
            );

            return newBankId;
        }

        public async Task UpdateAsync(Bank bankDto)
        {
            const string query = @"
        UPDATE Bank SET 
            BankName = @BankName,
            AccountHolderName = @AccountHolderName,
            AccountNo = @AccountNo,
            AccountType = @AccountType,
            Branch = @Branch,
            IFSC = @IFSC,
            Routing = @Routing,
            CurrencyId = @CurrencyId,
            CountryId = @CountryId,
            PrimaryContact = @PrimaryContact,
            Email = @Email,
            ContactNo = @ContactNo,
            Address = @Address,
            BudgetLineId = @BudgetLineId
        WHERE Id = @Id";

            await Connection.ExecuteAsync(query, bankDto);
        }


        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Bank SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
