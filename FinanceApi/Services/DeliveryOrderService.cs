using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using static FinanceApi.ModelDTO.DeliveryOrderDtos;

namespace FinanceApi.Services
{
    public class DeliveryOrderService : IDeliveryOrderService
    {
        private readonly IDeliveryOrderRepository _repo;
        public DeliveryOrderService(IDeliveryOrderRepository repo) => _repo = repo;

        public Task<int> CreateAsync(DoCreateRequest req, int userId) => _repo.CreateAsync(req, userId);

        public async Task<(DoHeaderEditDto? header, IEnumerable<DoLineDto> lines)> GetAsync(int id)
        {
            var hdr = await _repo.GetHeaderAsync(id);
            var lines = await _repo.GetLinesAsync(id);
            return (hdr, lines);
        }

        public Task<IEnumerable<DoHeaderDto>> GetAllAsync() => _repo.GetAllAsync();
        public Task<IEnumerable<DoLineDto>> GetLinesAsync(int doId) => _repo.GetLinesAsync(doId);
        public Task UpdateHeaderAsync(int id, DoUpdateHeaderRequest req, int userId) => _repo.UpdateHeaderAsync(id, req, userId);

        public Task<int> AddLineAsync(DoAddLineRequest req, int userId) => _repo.AddLineAsync(req, userId);
        public Task RemoveLineAsync(int lineId) => _repo.RemoveLineAsync(lineId);

        public Task SubmitAsync(int id, int userId) => _repo.SetStatusAsync(id, 1, userId); // 1=submitted
        public Task ApproveAsync(int id, int userId) => _repo.SetStatusAsync(id, 2, userId); // 2=approved
        public Task RejectAsync(int id, int userId) => _repo.SetStatusAsync(id, 3, userId); // 3=rejected
        public Task PostAsync(int id, int userId) => _repo.PostAsync(id, userId); // 4=posted
    }
}
