using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IPurchaseRequestRepository
    {
        Task<IEnumerable<PurchaseRequestDTO>> GetAllAsync();
        Task<PurchaseRequestDTO> GetByIdAsync(int id);
        Task<IEnumerable<PurchaseRequestDTO>> GetAvailablePurchaseRequestsAsync();
        Task<int> CreateAsync(PurchaseRequest pr);
        Task UpdateAsync(PurchaseRequest pr);
        Task DeactivateAsync(int id);
        Task<List<CreatedPrDto>> CreateFromReorderSuggestionsAsync(
     List<ReorderSuggestionGroupDto> groups,
     string requester,
     long requesterId,
     long? deptId,
     string? note,
     DateTime? headerDeliveryDate, long? stockReorderId);

        Task<int> CreateFromRecipeShortageAsync(CreatePrFromRecipeShortageRequest req);

    }
    }
