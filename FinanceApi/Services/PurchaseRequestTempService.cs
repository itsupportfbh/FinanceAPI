using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class PurchaseRequestTempService : IPurchaseRequestTempService
    {
        private readonly IPurchaseRequestTempRepository _repo;
        public PurchaseRequestTempService(IPurchaseRequestTempRepository repo) => _repo = repo;

        public async Task<int> CreateAsync(PurchaseRequestTempDto dto)
        {
            var t = Map(dto, isCreate: true);
            return await _repo.CreateAsync(t);
        }

        public async Task UpdateAsync(PurchaseRequestTempDto dto)
        {
            if (dto.Id <= 0) throw new ArgumentException("Id required");
            var t = Map(dto, isCreate: false);
            await _repo.UpdateAsync(t);
        }

        public Task<PurchaseRequestTemp> GetByIdAsync(int id) => _repo.GetByIdAsync(id);

        public Task<IEnumerable<PurchaseRequestTempDto>> ListAsync(int? departmentId = null)
            => _repo.ListAsync(departmentId);

        public Task DeleteAsync(int id, string userId) => _repo.DeleteAsync(id, userId);

        public Task<int> PromoteAsync(int tempId, string userId) => _repo.PromoteAsync(tempId, userId);

        private static PurchaseRequestTemp Map(PurchaseRequestTempDto d, bool isCreate)
        {
            return new PurchaseRequestTemp
            {
                Id = d.Id,
                Requester = d.Requester,
                DepartmentID = d.DepartmentID,
                DeliveryDate = d.DeliveryDate,
                MultiLoc = d.MultiLoc,
                Oversea = d.Oversea,
                PRLines = d.PRLines,
                Description = d.Description,
                Status = 0, // draft
                IsActive = d.IsActive,
                CreatedBy = d.UserId,
                UpdatedBy = d.UserId
            };
        }
    }

}
