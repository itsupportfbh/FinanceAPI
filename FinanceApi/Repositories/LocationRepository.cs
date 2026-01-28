using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using Dapper;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace FinanceApi.Repositories
{
    public class LocationRepository : DynamicRepository,ILocationRepository
    {
        private readonly ApplicationDbContext _context;

        public LocationRepository(IDbConnectionFactory connectionFactory)
     : base(connectionFactory)
        {
        }

        public async Task<IEnumerable<LocationDto>> GetAllAsync()
        {
            const string query = @" SELECT * from Location Where isActive = 1";

            return await Connection.QueryAsync<LocationDto>(query);
        }


        public async Task<IEnumerable<LocationDto>> GetAllLocationDetails()
        {
            const string query = @" Select l.*,c.CountryName, s.StateName, c1.CityName
   From Location as l
   inner join Country as c on c.Id = l.CountryId
   inner join State as s on s.Id = l.StateId
   inner join City as c1 on c1.Id = l.CityId
   Where l.IsActive = 1";

            return await Connection.QueryAsync<LocationDto>(query);
        }


        public async Task<LocationDto> GetByIdAsync(long id)
        {

            const string query = "SELECT * FROM Location WHERE Id = @Id";

            return await Connection.QuerySingleAsync<LocationDto>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(Location location)
        {
            const string query = @"INSERT INTO Location (Name,CountryId,StateId,CityId,ContactNumber,Latitude,Longitude,CreatedBy, CreatedDate, UpdatedBy, UpdatedDate,IsActive) 
                               OUTPUT INSERTED.Id 
                               VALUES (@Name,@CountryId,@StateId,@CityId,@ContactNumber,@Latitude,@Longitude,@CreatedBy, @CreatedDate, @UpdatedBy, @UpdatedDate,@IsActive)";
            return await Connection.QueryFirstAsync<int>(query, location);
        }


        public async Task UpdateAsync(Location location)
        {
            const string query = "UPDATE Location SET Name = @Name, CountryId = @CountryId, StateId = @StateId,CityId = @CityId,ContactNumber = @ContactNumber,Latitude = @Latitude,Longitude = @Longitude WHERE Id = @Id";
            await Connection.ExecuteAsync(query, location);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE Location SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }


        public async Task<IEnumerable<LocationDto>> GetLocationbyCountryId(long id)
        {

            const string query = "SELECT * FROM Location WHERE CountryId = @Id and IsActive = 1";

            return await Connection.QueryAsync<LocationDto>(query, new { Id = id });
        }

    }
}
