using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using static FinanceApi.ModelDTO.AllocationPreviewRequest;

namespace FinanceApi.Services
{
    public class SalesOrderService : ISalesOrderService
    {
        private readonly ISalesOrderRepository _repository;

        public SalesOrderService(ISalesOrderRepository repository)
        {
            _repository = repository;
        }

        public Task<IEnumerable<SalesOrderDTO>> GetAllAsync()
            => _repository.GetAllAsync();

        public Task<SalesOrderDTO?> GetByIdAsync(int id)
            => _repository.GetByIdAsync(id);

        public Task<int> CreateAsync(SalesOrder so)
        {
            if (so == null) throw new ArgumentNullException(nameof(so));
            if (so.LineItems == null || so.LineItems.Count == 0)
                throw new InvalidOperationException("Sales Order needs at least one line.");

            return _repository.CreateAsync(so);
        }

        public async Task UpdateAsync(SalesOrder so, bool reallocate)
        {
            if (so == null) throw new ArgumentNullException(nameof(so));
            if (so.Id <= 0) throw new ArgumentException("Invalid SO Id.", nameof(so.Id));

            if (reallocate)
                await _repository.UpdateWithReallocationAsync(so);
            else
                await _repository.UpdateAsync(so);
        }

        public Task DeactivateAsync(int id, int updatedBy)
            => _repository.DeactivateAsync(id, updatedBy);

        public Task<AllocationPreviewResponse> PreviewAllocationAsync(AllocationPreviewRequest req)
            => _repository.PreviewAllocationAsync(req);

        public Task<QutationDetailsViewInfo?> GetByQuatitonDetails(int quotationId)
            => _repository.GetByQuatitonDetails(quotationId);

        public async Task ApproveAsync(int id, int approvedBy)
        {
            var rows = await _repository.ApproveAsync(id, approvedBy);
            if (rows == 0)
                throw new KeyNotFoundException("Sales Order not found or inactive.");
        }

        public async Task RejectAsync(int id)
        {
            var rows = await _repository.RejectAsync(id);
            if (rows == 0)
                throw new KeyNotFoundException("Sales Order not found.");
        }

        public Task<IEnumerable<DraftLineDTO>> GetDraftLinesAsync()
    => _repository.GetDraftLinesAsync();

        public Task<IEnumerable<SalesOrderDTO>> GetAllByStatusAsync(byte status)
        {
            return _repository.GetAllByStatusAsync(status);
        }

        public Task<IEnumerable<SalesOrderListDto>> GetOpenByCustomerAsync(int customerId)
        {
            return _repository.GetOpenByCustomerAsync(customerId);
        }
    }
}
