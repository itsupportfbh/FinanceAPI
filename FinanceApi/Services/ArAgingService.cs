public class ArAgingService : IArAgingService
{
    private readonly IArAgingRepository _repo;

    public ArAgingService(IArAgingRepository repo)
    {
        _repo = repo;
    }

    public Task<IEnumerable<ArAgingSummaryDto>> GetSummaryAsync(DateTime fromDate, DateTime toDate)
        => _repo.GetSummaryAsync(fromDate, toDate);

    public Task<IEnumerable<ArAgingInvoiceDto>> GetCustomerInvoicesAsync(int customerId, DateTime fromDate, DateTime toDate)
        => _repo.GetCustomerInvoicesAsync(customerId, fromDate, toDate);
}
