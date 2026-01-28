using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;

public class ArCollectionForecastService : IArCollectionForecastService
{
    private readonly IArCollectionForecastRepository _repo;

    public ArCollectionForecastService(IArCollectionForecastRepository repo)
    {
        _repo = repo;
    }

    public Task<IEnumerable<ArCollectionForecastSummaryDto>> GetSummaryAsync(
        DateTime? fromDate,
        DateTime? toDate)
        => _repo.GetSummaryAsync(fromDate, toDate);

    public Task<IEnumerable<ArCollectionForecastDetailDto>> GetDetailAsync(
        int customerId,
        DateTime? fromDate,
        DateTime? toDate)
        => _repo.GetDetailAsync(customerId, fromDate, toDate);
}