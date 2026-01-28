using FinanceApi.Interfaces;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class DepartmentService : IDepartmentService
    {
        private readonly IDepartmentRepository _repository;

        public DepartmentService(IDepartmentRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<Department>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<Department?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<int> CreateAsync(Department department)
        {

            return await _repository.CreateAsync(department);
        }

        public async Task UpdateAsync(Department department)
        {
            await _repository.UpdateAsync(department);
        }

        public async Task DeleteLicense(int id)
        {
             await _repository.DeactivateAsync(id);
        }
    }
}
