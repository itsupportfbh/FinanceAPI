using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IVehicleRepository
    {
        Task<IEnumerable<VehicleDTO>> GetAllAsync(bool onlyActive = true);
        Task<VehicleDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(Vehicle vehicle);
        Task UpdateAsync(Vehicle vehicle);
        Task DeactivateAsync(int id);
        Task<bool> ExistsByVehicleNoAsync(string vehicleNo);
    }
}
