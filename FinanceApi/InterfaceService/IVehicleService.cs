using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IVehicleService
    {
        Task<IEnumerable<VehicleDTO>> GetAllAsync();
        Task<VehicleDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(Vehicle vehicle);
        Task UpdateAsync(Vehicle vehicle);
        Task DeactivateAsync(int id);
    }
}
