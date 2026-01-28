using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ICustomerGroupsRepository
    {
        Task<IEnumerable<CustomerGroupsDTO>> GetAllAsync();
        Task<CustomerGroupsDTO> GetByIdAsync(long id);
        Task<int> CreateAsync(CustomerGroups customerGroups);
        Task UpdateAsync(CustomerGroups customerGroups);
        Task DeactivateAsync(int id);
    }
}
