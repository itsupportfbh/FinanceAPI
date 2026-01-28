using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ICustomerGroupsService
    {
        Task<IEnumerable<CustomerGroupsDTO>> GetAllAsync();
        Task<CustomerGroupsDTO> GetById(int id);
        Task<int> CreateAsync(CustomerGroups customerGroups);
        Task UpdateAsync(CustomerGroups customerGroups);
        Task DeleteAsync(int id);
    }
}
