using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class VehicleService:IVehicleService

    {
        private readonly IVehicleRepository _repo;
        public VehicleService(IVehicleRepository repo) => _repo = repo;

        public Task<IEnumerable<VehicleDTO>> GetAllAsync() => _repo.GetAllAsync(true);
        public Task<VehicleDTO?> GetByIdAsync(int id) => _repo.GetByIdAsync(id);

        public async Task<int> CreateAsync(Vehicle v)
        {
            // Optional: uniqueness guard
            if (await _repo.ExistsByVehicleNoAsync(v.VehicleNo))
                throw new InvalidOperationException("Vehicle number already exists.");
            return await _repo.CreateAsync(v);
        }

        public Task UpdateAsync(Vehicle v) => _repo.UpdateAsync(v);
        public Task DeactivateAsync(int id) => _repo.DeactivateAsync(id);
    }
}
