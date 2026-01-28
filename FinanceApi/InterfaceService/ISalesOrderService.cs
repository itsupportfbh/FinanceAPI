using FinanceApi.ModelDTO;
using FinanceApi.Models;
using static FinanceApi.ModelDTO.AllocationPreviewRequest;
using static FinanceApi.ModelDTO.QutationDetailsViewInfo;

namespace FinanceApi.InterfaceService
{
    public interface ISalesOrderService
    {
        Task<IEnumerable<SalesOrderDTO>> GetAllAsync();
        Task<SalesOrderDTO?> GetByIdAsync(int id);

        Task<int> CreateAsync(SalesOrder so);
        Task UpdateAsync(SalesOrder so, bool reallocate);
        Task DeactivateAsync(int id, int updatedBy);

        Task<QutationDetailsViewInfo?> GetByQuatitonDetails(int quotationId);
        Task<AllocationPreviewResponse> PreviewAllocationAsync(AllocationPreviewRequest req);

        // NEW:
        Task ApproveAsync(int id, int approvedBy);
        Task RejectAsync(int id);

        Task<IEnumerable<DraftLineDTO>> GetDraftLinesAsync();
        Task<IEnumerable<SalesOrderDTO>> GetAllByStatusAsync(byte status);
        Task<IEnumerable<SalesOrderListDto>> GetOpenByCustomerAsync(int customerId);
    }
}
