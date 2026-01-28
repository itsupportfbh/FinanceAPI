using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class DriverService : IDriverService
    {
        private readonly IDriverRepository _driverRepository;
        public DriverService(IDriverRepository driverRepository)
        { 
            _driverRepository = driverRepository;
        }
        public async Task<IEnumerable<DriverDTO>> GetAllAsync()
        {
            return await _driverRepository.GetAllAsync();
        }

        // Create New Driver
        public async Task<int> CreateAsync(Driver driver)
        {
            return await _driverRepository.CreateAsync(driver);
        }

        // Get Driver By ID
        public async Task<DriverDTO> GetById(int id)
        {
            return await _driverRepository.GetByIdAsync(id);
        }

        // Update Driver
        public async Task UpdateAsync(Driver driver)
        {
            await _driverRepository.UpdateAsync(driver);
        }

        // Deactivate Driver
        public async Task DeleteAsync(int id)
        {
            await _driverRepository.DeactivateAsync(id);
        }
    }
}
