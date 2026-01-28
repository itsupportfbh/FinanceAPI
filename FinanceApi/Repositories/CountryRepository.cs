using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;


namespace FinanceApi.Repositories
{
    public class CountryRepository : DynamicRepository, ICountryRepository
    {
        private readonly ApplicationDbContext _context;

        public CountryRepository(IDbConnectionFactory connectionFactory)
       : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<Country>> GetAllAsync()
        {
            const string query = @"
                SELECT * from Country Where isActive = 'true' ";

            return await Connection.QueryAsync<Country>(query);
        }


        public async Task<Country> GetByIdAsync(int id)
        {

            const string query = "SELECT * FROM Country WHERE Id = @Id";

            return await Connection.QuerySingleAsync<Country>(query, new { Id = id });
        }
        public async Task<int> CreateAsync(Country country)
        {
            const string query = @"
        INSERT INTO Country 
            (CountryName, GSTPercentage , CreatedBy, CreatedDate, UpdatedBy, UpdatedDate, IsActive) 
        OUTPUT INSERTED.Id 
        VALUES 
            (@CountryName, @GSTPercentage, @CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate, @IsActive)
    ";

            var parameters = new DynamicParameters();
            parameters.Add("@CountryName", country.CountryName);
            parameters.Add("@GSTPercentage", country.GSTPercentage);
            parameters.Add("@CreatedBy", country.CreatedBy);
            parameters.Add("@CreatedDate", country.CreatedDate);
            parameters.Add("@UpdatedBy", country.UpdatedBy);
            parameters.Add("@UpdatedDate", country.UpdatedDate);
            parameters.Add("@IsActive", country.IsActive);

            return await Connection.QueryFirstAsync<int>(query, parameters);
        }




        public async Task UpdateAsync(Country country)
        {
            const string query = "UPDATE Country SET CountryName = @CountryName,GSTPercentage = @GSTPercentage WHERE Id = @Id";
            await Connection.ExecuteAsync(query, country);
        }

        public async Task<Country> GetByNameAsync(string name)
        {

            const string query = "SELECT * FROM Country WHERE CountryName = @CountryName and IsActive=1";

            return await Connection.QuerySingleOrDefaultAsync<Country>(query, new { CountryName = name });
        }

        public async Task<bool> NameExistsAsync(string CountryName, int excludeId)
        {
            const string sql = @"
        SELECT 1
        FROM Country
        WHERE IsActive = 1
          AND Id <> @excludeId
          AND UPPER(LTRIM(RTRIM(CountryName))) = UPPER(LTRIM(RTRIM(@CountryName)))";

            var found = await Connection.QueryFirstOrDefaultAsync<int?>(sql, new { CountryName, excludeId });
            return found.HasValue;
        }
        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Country SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
