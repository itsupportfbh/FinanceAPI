using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class PurchaseRequestService : IPurchaseRequestService
    {
        private readonly IPurchaseRequestRepository _repository;

        public PurchaseRequestService(IPurchaseRequestRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<PurchaseRequestDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(PurchaseRequest purchaseRequest)
        {
            return await _repository.CreateAsync(purchaseRequest);

        }

        public async Task<PurchaseRequestDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<IEnumerable<PurchaseRequestDTO>> GetAvailablePurchaseRequestsAsync()
        {
            return await _repository.GetAvailablePurchaseRequestsAsync();
        }

        public Task UpdateAsync(PurchaseRequest purchaseRequest)
        {
            return _repository.UpdateAsync(purchaseRequest);
        }


        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }
        public async Task<List<CreatedPrDto>> CreateFromReorderSuggestionsAsync(
     CreateReorderSuggestionsRequest req,
     string requesterName,
     long requesterId,
     long? departmentId,
     DateTime? deliveryDate, long? stockReorderId)
        {
            return await _repository.CreateFromReorderSuggestionsAsync(
                groups: req.Groups,
                requester: requesterName,                 // map to repo param 'requester'
                requesterId: requesterId,
                deptId: departmentId,                     // map to repo param 'deptId'
                note: req.Note,
                headerDeliveryDate: deliveryDate ,stockReorderId:stockReorderId         // map to repo param 'headerDeliveryDate'
            );
        }

        public Task<int> CreateFromRecipeShortageAsync(CreatePrFromRecipeShortageRequest req)
        => _repository.CreateFromRecipeShortageAsync(req);

    }
}
