// Services/SalesInvoiceService.cs
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using static FinanceApi.ModelDTO.SalesInvoiceDtos;

namespace FinanceApi.Services
{
    public class SalesInvoiceService : ISalesInvoiceService
    {
        private readonly ISalesInvoiceRepository _repo;
        public SalesInvoiceService(ISalesInvoiceRepository repo) => _repo = repo;

        public Task<IEnumerable<SiSourceLineDto>> GetSourceLinesAsync(byte sourceType, int sourceId)
            => _repo.GetSourceLinesAsync(sourceType, sourceId);

        public Task<int> CreateAsync(int userId, SiCreateRequest req)
            => _repo.CreateAsync(userId, req);

        public Task<SiHeaderDto?> GetAsync(int id) => _repo.GetAsync(id);
        public Task<IEnumerable<SiLineDto>> GetLinesAsync(int id) => _repo.GetLinesAsync(id);
        public Task<IEnumerable<SiListRowDto>> GetListAsync() => _repo.GetListAsync();
        public Task DeleteAsync(int id) => _repo.DeactivateAsync(id);
        public Task UpdateHeaderAsync(int id, DateTime invoiceDate, int userId)
            => _repo.UpdateHeaderAsync(id, invoiceDate, userId);

        public Task<int> AddLineAsync(int siId, SiCreateLine l, byte sourceType)
            => _repo.AddLineAsync(siId, l, sourceType);

        public Task UpdateLineAsync(int lineId, decimal qty, decimal unitPrice, decimal discountPct, decimal gstPct, string tax, int? taxCodeId,decimal? lineAmount, decimal? taxAmount, string? description,int?budgetLineId,int userId)
            => _repo.UpdateLineAsync(lineId, qty, unitPrice, discountPct, gstPct, tax, taxCodeId,lineAmount, taxAmount,description, budgetLineId, userId);

        public Task RemoveLineAsync(int lineId) => _repo.RemoveLineAsync(lineId);
    }
}
