using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IServicesService
    {
        Task<IEnumerable<ServiceDTO>> GetAllAsync();
        Task<ServiceDTO> GetById(int id);
        Task<int> CreateAsync(Service service);
        Task UpdateAsync(Service service);
        Task DeleteAsync(int id);
    }
}
