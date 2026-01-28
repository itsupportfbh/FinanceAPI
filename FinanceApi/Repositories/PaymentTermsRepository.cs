using FinanceApi.Data;
using Dapper;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class PaymentTermsRepository : DynamicRepository,IPaymentTermsRepository
    {
        private readonly ApplicationDbContext _context;
        public PaymentTermsRepository(IDbConnectionFactory connectionFactory)
        : base(connectionFactory)
        {
        }
        public async Task<IEnumerable<PaymentTermsDTO>> GetAllAsync()
        {
            const string query = @"
                SELECT * from PaymentTerms";

            return await Connection.QueryAsync<PaymentTermsDTO>(query);
        }


        public async Task<PaymentTermsDTO> GetByIdAsync(int id)
        {

            const string query = "SELECT * FROM PaymentTerms WHERE Id = @Id";

            return await Connection.QuerySingleAsync<PaymentTermsDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(PaymentTerms paymentTermsDTO)
        {
            const string query = @"INSERT INTO PaymentTerms (PaymentTermsName,Description,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@PaymentTermsName,@Description,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, paymentTermsDTO);
        }


        public async Task UpdateAsync(PaymentTerms paymentTermsDTO)
        {
            const string query = "UPDATE PaymentTerms SET PaymentTermsName = @PaymentTermsName, Description = @Description WHERE Id = @Id";
            await Connection.ExecuteAsync(query, paymentTermsDTO);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE PaymentTerms SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }

    }
}
