using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IDepartmentRepository
    {
        Task<IEnumerable<Department>> GetAllAsync();
        Task<Department> GetByIdAsync(int id);
        Task<int> CreateAsync(Department department);
        Task UpdateAsync(Department department);
        Task DeactivateAsync(int id);
    }
}