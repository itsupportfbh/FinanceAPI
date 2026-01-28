using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IBinRepository
    {
        Task<IEnumerable<BinDTO>> GetAllAsync();
        Task<BinDTO> GetByIdAsync(long id);
        Task<int> CreateAsync(Bin BinDTO);
        Task UpdateAsync(Bin BinDTO);
        Task DeactivateAsync(int id);
    }
}
