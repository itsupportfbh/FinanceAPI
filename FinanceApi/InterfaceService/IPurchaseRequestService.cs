using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IPurchaseRequestService
    {
        Task<IEnumerable<PurchaseRequestDTO>> GetAllAsync();
        Task<PurchaseRequestDTO> GetById(int id);
        Task<IEnumerable<PurchaseRequestDTO>> GetAvailablePurchaseRequestsAsync();
        Task<int> CreateAsync(PurchaseRequest purchaseRequest);
        Task UpdateAsync(PurchaseRequest purchaseRequest);
        Task DeleteLicense(int id);
        Task<List<CreatedPrDto>> CreateFromReorderSuggestionsAsync(
      CreateReorderSuggestionsRequest req,
      string requesterName,
      long requesterId,
      long? departmentId,
      DateTime? deliveryDate, long? stockReorderId);

        Task<int> CreateFromRecipeShortageAsync(CreatePrFromRecipeShortageRequest req);

    }
}
