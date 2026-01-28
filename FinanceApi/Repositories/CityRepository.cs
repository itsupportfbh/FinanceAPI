using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class CityRepository : DynamicRepository,ICityRepository
    {
        private readonly ApplicationDbContext _context;

        public CityRepository(IDbConnectionFactory connectionFactory)
        : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<CityDto>> GetAllAsync()
        {
            const string query = @" SELECT * from City Where isActive = 1";

            return await Connection.QueryAsync<CityDto>(query);
        }


        public async Task<CityDto> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM City WHERE Id = @Id";

            return await Connection.QuerySingleAsync<CityDto>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(City city)
        {
            const string query = @"INSERT INTO City (CityName,StateId,CountryId,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@CityName,@StateId,@CountryId,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, city);
        }


        public async Task UpdateAsync(City city)
        {
            const string query = @"UPDATE City 
                       SET CountryId = @CountryId, StateId = @StateId, CityName = @CityName
                       WHERE Id = @Id";
            await Connection.ExecuteAsync(query, city);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE City SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }


        public async Task<IEnumerable<CityDto>> GetStateWithCountryId(int id)
        {
            const string query = @"
        SELECT s.Id, s.StateName 
        FROM State AS s 
        INNER JOIN Country AS c ON c.Id = s.CountryId 
        WHERE c.Id = @id AND s.isActive = 1";

            return await Connection.QueryAsync<CityDto>(query, new { Id = id });
        }


        public async Task<IEnumerable<CityDto>> GetCityWithStateId(int id)
        {
            const string query = @"
        SELECT c.Id, c.cityName
FROM City AS c 
INNER JOIN State AS s ON s.Id = c.StateId 
WHERE c.StateId = @id and c.isActive = 1";

            return await Connection.QueryAsync<CityDto>(query, new { Id = id });
        }
        public async Task<bool> NameExistsAsync(string name, int countryId, int excludeId)
        {
            const string sql = @"
        SELECT 1
        FROM City
        WHERE IsActive = 1
          AND CountryId = @countryId
          AND Id <> @excludeId
          AND UPPER(LTRIM(RTRIM(CityName))) = UPPER(LTRIM(RTRIM(@name)))";

            var hit = await Connection.QueryFirstOrDefaultAsync<int?>(sql, new { name, countryId, excludeId });
            return hit.HasValue;
        }


        public async Task<CityDto?> GetByNameInCountryAsync(string name, int countryId)
        {
            const string sql = @"
        SELECT TOP 1 *
        FROM City
        WHERE IsActive = 1
          AND CountryId = @countryId
          AND UPPER(LTRIM(RTRIM(Name))) = UPPER(LTRIM(RTRIM(@name)))";

            return await Connection.QueryFirstOrDefaultAsync<CityDto>(sql, new { name, countryId });
        }

    }
}
