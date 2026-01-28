using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IDepartmentService
    {
        Task<IEnumerable<Department>> GetAllAsync();
        Task<Department> GetByIdAsync(int id);
        Task<int> CreateAsync(Department department);
        Task UpdateAsync(Department department);
        Task DeleteLicense(int id);
    }
}
