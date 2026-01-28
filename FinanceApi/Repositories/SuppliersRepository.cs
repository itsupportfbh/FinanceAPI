using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class SuppliersRepository : DynamicRepository,ISuppliersRepository
    {
        private readonly ApplicationDbContext _context;

        // Add new supplier
        public SuppliersRepository(IDbConnectionFactory connectionFactory)
      : base(connectionFactory)
        {
        }
        public async Task<IEnumerable<SuppliersDTO>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Suppliers where isActive =1";

            return await Connection.QueryAsync<SuppliersDTO>(query);
        }


        public async Task<SuppliersDTO> GetByIdAsync(int id)
        {

            const string query = "SELECT * FROM Suppliers WHERE Id = @Id";

            return await Connection.QuerySingleAsync<SuppliersDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Suppliers supplier)
        {
            // Step 1: Get the last numeric part of the supplier code
            const string getLastCodeQuery = @"
SELECT TOP 1 
    TRY_CAST(SUBSTRING(Code, 4, LEN(Code)) AS INT) AS CodeNumber
FROM Suppliers
WHERE ISNUMERIC(SUBSTRING(Code, 4, LEN(Code))) = 1
ORDER BY CodeNumber DESC";

            int lastCodeNumber = await Connection.QueryFirstOrDefaultAsync<int?>(getLastCodeQuery) ?? 0;

            // Step 2: Calculate next code number
            int nextCodeNumber = lastCodeNumber + 1;

            // Step 3: Format code as SR-0001, SR-0002, etc.
            supplier.Code = $"SR-{nextCodeNumber:D4}";

            // Step 4: Handle DocumentDetails (already populated as JSON from Angular)
            // No need to serialize here if Angular sends JSON string

            // Step 5: Define insert query (FileID/FileName removed, DocumentDetails added)
            const string insertQuery = @"
INSERT INTO Suppliers (
    Name, Code, StatusId, LeadTime,CountryId, TermsId, CurrencyId,
    TaxReg, IncotermsId, Contact, Email, Phone, Address,
    BankName, BankAcc, BankSwift, BankBranch,
    CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive,
    ItemID, ComplianceDocuments,BudgetLineId
)
OUTPUT INSERTED.Id
VALUES (
    @Name, @Code, @StatusId, @LeadTime,@CountryId, @TermsId, @CurrencyId,
    @TaxReg, @IncotermsId, @Contact, @Email, @Phone, @Address,
    @BankName, @BankAcc, @BankSwift, @BankBranch,
    @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive,
    @ItemID, @ComplianceDocuments,@BudgetLineId
);";

            // Step 6: Execute insert and return new ID
            return await Connection.QueryFirstAsync<int>(insertQuery, supplier);
        }




        public async Task UpdateAsync(Suppliers supplierDto)
        {
            const string query = @"
    UPDATE Suppliers
    SET
        Name = @Name,
        Code = @Code,
        StatusId = @StatusId,
        LeadTime = @LeadTime,
        CountryId = @CountryId,
        TermsId = @TermsId,
        CurrencyId = @CurrencyId,
        TaxReg = @TaxReg,
        IncotermsId = @IncotermsId,
        Contact = @Contact,
        Email = @Email,
        Phone = @Phone,
        Address = @Address,
        BankName = @BankName,
        BankAcc = @BankAcc,
        BankSwift = @BankSwift,
        BankBranch = @BankBranch,
        UpdatedBy = @UpdatedBy,
        UpdatedDate = @UpdatedDate,
        ItemID = @ItemID,
        ComplianceDocuments = @ComplianceDocuments,
BudgetLineId = @BudgetLineId
    WHERE Id = @Id;
    ";

            await Connection.ExecuteAsync(query, supplierDto);
        }



        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Suppliers SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }


    }
}
