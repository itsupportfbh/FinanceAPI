using FinanceApi.Data;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using FinanceApi.Interfaces;
using Dapper;
using System.Diagnostics.Metrics;

namespace FinanceApi.Repositories
{
    public class RegionRepository : DynamicRepository,IRegionRepository
    {
        private readonly ApplicationDbContext _context;



        public RegionRepository(IDbConnectionFactory connectionFactory)
            : base(connectionFactory)
        {
            
        }

        public async Task<IEnumerable<Region>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Region Where isActive = 'true' ";

            return await Connection.QueryAsync<Region>(query);
        }

        public async Task<Region?> GetByIdAsync(int id)
        {
            const string query = "SELECT * FROM Region WHERE Id = @Id";

            return await Connection.QuerySingleAsync<Region>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Region region)
        {
            const string query = @"INSERT INTO Region (RegionName,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@RegionName,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, region);
        }

        public async Task UpdateAsync(Region updatedRegion)
        {
            const string query = "UPDATE Region SET RegionName = @RegionName  WHERE Id = @Id";
            await Connection.ExecuteAsync(query, updatedRegion);
        }


        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Region SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
