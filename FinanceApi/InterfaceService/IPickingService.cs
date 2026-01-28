using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IPickingService
    {
        Task<IEnumerable<PickingDTO>> GetAllAsync();
        Task<PickingDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(Picking picking);
        Task UpdateAsync(Picking picking);
        Task DeactivateAsync(int id, int updatedBy);

        // codes
        (string barCode, string qrCode) GenerateCodes(int soId, DateTime? soDateUtc = null);

        Task<CodesResponseEx> GenerateCodesAsync(CodesRequest req);
    }
}
