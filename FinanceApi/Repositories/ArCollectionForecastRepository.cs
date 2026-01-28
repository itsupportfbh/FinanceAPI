using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Repositories;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using System.Data;
using Dapper;

public class ArCollectionForecastRepository
    : DynamicRepository, IArCollectionForecastRepository
{
    public ArCollectionForecastRepository(IDbConnectionFactory f) : base(f) { }

    public async Task<IEnumerable<ArCollectionForecastSummaryDto>> GetSummaryAsync(
        DateTime? fromDate,
        DateTime? toDate)
    {
        return await Connection.QueryAsync<ArCollectionForecastSummaryDto>(
            "sp_ArCollectionsForecastSummary",
            new { FromDate = fromDate, ToDate = toDate },
            commandType: CommandType.StoredProcedure);
    }

    public async Task<IEnumerable<ArCollectionForecastDetailDto>> GetDetailAsync(
        int customerId,
        DateTime? fromDate,
        DateTime? toDate)
    {
        return await Connection.QueryAsync<ArCollectionForecastDetailDto>(
            "sp_ArCollectionsForecastDetail",
            new { CustomerId = customerId, FromDate = fromDate, ToDate = toDate },
            commandType: CommandType.StoredProcedure);
    }
}
