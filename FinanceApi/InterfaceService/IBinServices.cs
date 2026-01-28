using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IBinServices
    {
        Task<IEnumerable<BinDTO>> GetAllAsync();
        Task<int> CreateAsync(Bin BinDTO);
        Task<BinDTO> GetById(long id);
        Task UpdateAsync(Bin BinDTO);
        Task DeleteAsync(int id);
    }
}
