using FinanceApi.Models;
using FinanceApi.Data;
using Microsoft.EntityFrameworkCore;
using FinanceApi.Interfaces;
using Dapper;

namespace FinanceApi.Repositories
{
    public class DepartmentRepository : DynamicRepository,IDepartmentRepository
    {
        private readonly ApplicationDbContext _context;

        public DepartmentRepository(IDbConnectionFactory connectionFactory)
         : base(connectionFactory)
        {

        }

        public async Task<IEnumerable<Department>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Department Where isActive = 'true' ";

            return await Connection.QueryAsync<Department>(query);
        }

        public async Task<Department?> GetByIdAsync(int id)
        {
            const string query = "SELECT * FROM Department WHERE Id = @Id";

            return await Connection.QuerySingleAsync<Department>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Department department)
        {
            const string query = @"INSERT INTO Department (DepartmentCode,DepartmentName,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@DepartmentCode,@DepartmentName,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, department);
        }

        public async Task UpdateAsync(Department updatedDepartment)
        {
            const string query = "UPDATE Department SET DepartmentCode = @DepartmentCode,DepartmentName = @DepartmentName WHERE Id = @Id";
            await Connection.ExecuteAsync(query, updatedDepartment);

        }


        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Department SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}

