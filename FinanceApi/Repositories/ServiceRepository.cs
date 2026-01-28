using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;



namespace FinanceApi.Repositories
{
    public class ServiceRepository : DynamicRepository,IServicesRepository
    {
        private readonly ApplicationDbContext _context;

        public ServiceRepository(IDbConnectionFactory connectionFactory)
        : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<ServiceDTO>> GetAllAsync()
        {
            const string query = @" SELECT * from Service Where isActive = 1";

            return await Connection.QueryAsync<ServiceDTO>(query);
        }


        public async Task<ServiceDTO> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM Service WHERE Id = @Id";

            return await Connection.QuerySingleAsync<ServiceDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Service service)
        {
            const string query = @"INSERT INTO Service (Name,Charge,Tax,Description,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@Name,@Charge,@Tax,@Description,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, service);
        }


        public async Task UpdateAsync(Service service)
        {
            const string query = "UPDATE Service SET Name = @Name,Charge = @Charge,Tax = @Tax,Description = @Description WHERE Id = @Id";
            await Connection.ExecuteAsync(query, service);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Service SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
