using FinanceApi.ModelDTO;
using static FinanceApi.ModelDTO.DeliveryOrderDtos;

namespace FinanceApi.Interfaces
{
    public interface IDeliveryOrderRepository
    {
        Task<int> CreateAsync(DoCreateRequest req, int userId);
        Task<IEnumerable<DoHeaderDto>> GetAllAsync();
        Task<DoHeaderEditDto?> GetHeaderAsync(int id);
        Task<IEnumerable<DoLineDto>> GetLinesAsync(int doId);
        Task UpdateHeaderAsync(int id, DoUpdateHeaderRequest req, int userId);

        Task<int> AddLineAsync(DoAddLineRequest req, int userId);
        Task RemoveLineAsync(int lineId);

        Task SetStatusAsync(int id, int status, int userId);
        Task PostAsync(int id, int userId);

        Task<IEnumerable<object>> GetSoRedeliveryViewAsync(int doId, int soId);
    }
}
