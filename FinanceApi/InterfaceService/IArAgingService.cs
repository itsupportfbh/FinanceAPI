public interface IArAgingService
{
    Task<IEnumerable<ArAgingSummaryDto>> GetSummaryAsync(DateTime fromDate, DateTime toDate);
    Task<IEnumerable<ArAgingInvoiceDto>> GetCustomerInvoicesAsync(int customerId, DateTime fromDate, DateTime toDate);
}