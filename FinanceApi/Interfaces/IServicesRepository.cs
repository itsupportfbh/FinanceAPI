using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IServicesRepository
    {
        Task<IEnumerable<ServiceDTO>> GetAllAsync();
        Task<ServiceDTO> GetByIdAsync(long id);
        Task<int> CreateAsync(Service service);
        Task UpdateAsync(Service service);
        Task DeactivateAsync(int id);
    }
}
