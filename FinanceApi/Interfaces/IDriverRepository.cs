using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IDriverRepository
    {
        Task<IEnumerable<DriverDTO>> GetAllAsync();
        Task<DriverDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(Driver dto);
        Task UpdateAsync(Driver dto);
        Task DeactivateAsync(int id);
    }
}
