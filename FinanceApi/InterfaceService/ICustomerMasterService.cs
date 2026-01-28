using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Threading.Tasks;

namespace FinanceApi.InterfaceService
{
    public interface ICustomerMasterService
    {
        Task<IEnumerable<CustomerMasterDTO>> GetAllAsync();
        Task<int> CreateAsync(CustomerMaster customerMaster);
        Task<CustomerMasterDTO> GetById(int id);
        Task UpdateAsync(UpdateCustomerRequest req);
        Task<IEnumerable<CustomerList>> GetAllCustomerDetails();

        Task<CustomerList> EditLoadforCustomerbyId(int id);

        Task<bool> DeactivateAsync(int customerId, int? kycId);
    }
}
