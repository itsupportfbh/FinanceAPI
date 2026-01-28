// InterfaceService/ISalesInvoiceService.cs
using static FinanceApi.ModelDTO.SalesInvoiceDtos;

namespace FinanceApi.InterfaceService
{
    public interface ISalesInvoiceService
    {
        Task<IEnumerable<SiSourceLineDto>> GetSourceLinesAsync(byte sourceType, int sourceId);
        Task<int> CreateAsync(int userId, SiCreateRequest req);
        Task<SiHeaderDto?> GetAsync(int id);
        Task<IEnumerable<SiLineDto>> GetLinesAsync(int id);
        Task<IEnumerable<SiListRowDto>> GetListAsync();
        Task DeleteAsync(int id);
        Task UpdateHeaderAsync(int id, DateTime invoiceDate, int userId);
        Task<int> AddLineAsync(int siId, SiCreateLine l, byte sourceType);
        Task UpdateLineAsync(int lineId, decimal qty, decimal unitPrice, decimal discountPct, decimal gstPct, string tax, int? taxCodeId, decimal? lineAmount, decimal? taxAmount, string? description,int?budgetLineId ,int userId); // <— desc
        Task RemoveLineAsync(int lineId);

    }
}
