// Repositories/RunningNumberRepository.cs
using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using System.Data;

namespace FinanceApi.Repositories
{
    public class RunningNumberRepository : DynamicRepository, IRunningNumberRepository
    {
        public RunningNumberRepository(IDbConnectionFactory factory) : base(factory) { }

        public async Task<int> NextPickingSerialAsync(string dateKey)
        {
            const string sql = @"
MERGE dbo.PickingSerial AS T
USING (SELECT @DateKey AS DateKey) AS S
ON (T.DateKey = S.DateKey)
WHEN MATCHED THEN
  UPDATE SET LastNumber = T.LastNumber + 1
WHEN NOT MATCHED THEN
  INSERT (DateKey, LastNumber) VALUES (S.DateKey, 1)
OUTPUT inserted.LastNumber;";
            return await Connection.ExecuteScalarAsync<int>(sql, new { DateKey = dateKey });
        }
    }
}
