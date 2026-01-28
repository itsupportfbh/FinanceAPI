using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ICatagoryRepository
    {
        Task<IEnumerable<CatagoryDTO>> GetAllAsync();
        Task<CatagoryDTO> GetByIdAsync(long id);
        Task<int> CreateAsync(Catagory catagoryDTO);
        Task UpdateAsync(CatagoryDTO catagoryDTO);
        Task DeactivateAsync(int id);
    }
}
