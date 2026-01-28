// File: InterfaceService/IDeliveryOrderService.cs
using static FinanceApi.ModelDTO.DeliveryOrderDtos;

namespace FinanceApi.InterfaceService
{
    public interface IDeliveryOrderService
    {
        Task<int> CreateAsync(DoCreateRequest req, int userId);
        Task<(DoHeaderEditDto? header, IEnumerable<DoLineDto> lines)> GetAsync(int id);
        Task<IEnumerable<DoHeaderDto>> GetAllAsync();
        Task<IEnumerable<DoLineDto>> GetLinesAsync(int doId);
        Task UpdateHeaderAsync(int id, DoUpdateHeaderRequest req, int userId);

        Task<int> AddLineAsync(DoAddLineRequest req, int userId);
        Task RemoveLineAsync(int lineId);

        Task SubmitAsync(int id, int userId);
        Task ApproveAsync(int id, int userId);
        Task RejectAsync(int id, int userId);
        Task PostAsync(int id, int userId);
    }
}
