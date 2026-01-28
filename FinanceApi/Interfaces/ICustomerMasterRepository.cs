using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface ICustomerMasterRepository
    {
        Task<IEnumerable<CustomerMasterDTO>> GetAllAsync();
        Task<CustomerMasterDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(CustomerMaster customerMaster);
        Task<bool> UpdateAsync(UpdateCustomerRequest req);
        Task<bool> DeactivateAsync(int customerId, int? kycId);
        Task<IEnumerable<CustomerList>> GetAllCustomerDetails();

        Task<CustomerList> EditLoadforCustomerbyId(int id);
    }
}
