using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IPickingRepository
    {
        Task<IEnumerable<PickingDTO>> GetAllAsync();
        Task<PickingDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(Picking picking);
        Task UpdateAsync(Picking picking);
        Task DeactivateAsync(int id, int updatedBy);
    }
}
