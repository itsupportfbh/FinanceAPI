using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ICustomerRepository
    {
        Task<List<CustomerDto>> GetAllAsync();
        Task<CustomerDto?> GetByIdAsync(int id);
        Task<Customer> CreateAsync(Customer customer);
        Task<Customer?> UpdateAsync(int id, Customer customer);
        Task<bool> DeleteAsync(int id);
    }
}
