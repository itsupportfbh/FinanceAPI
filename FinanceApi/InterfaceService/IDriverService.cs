using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IDriverService
    {
        Task<IEnumerable<DriverDTO>> GetAllAsync();
        Task<int> CreateAsync(Driver driver);
        Task<DriverDTO> GetById(int id);
        Task UpdateAsync(Driver driver);
        Task DeleteAsync(int id);
    }
}
