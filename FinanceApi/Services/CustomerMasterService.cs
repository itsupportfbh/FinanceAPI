using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class CustomerMasterService : ICustomerMasterService
    {
        private readonly ICustomerMasterRepository _customerMasterRepository;

        public CustomerMasterService(ICustomerMasterRepository customerMasterRepository)
        {
            _customerMasterRepository = customerMasterRepository;
        }

        public async Task<IEnumerable<CustomerMasterDTO>> GetAllAsync()
        {
            return await _customerMasterRepository.GetAllAsync();
        }

        public async Task<int> CreateAsync(CustomerMaster customerMaster)
        {
            return await _customerMasterRepository.CreateAsync(customerMaster);

        }

        public async Task<CustomerMasterDTO> GetById(int id)
        {
            return await _customerMasterRepository.GetByIdAsync(id);
        }

        public Task UpdateAsync(UpdateCustomerRequest req)
        {
            return _customerMasterRepository.UpdateAsync(req);
        }


        public Task<bool> DeactivateAsync(int customerId, int? kycId)
         => _customerMasterRepository.DeactivateAsync(customerId, kycId);

        public async Task<IEnumerable<CustomerList>> GetAllCustomerDetails()
        {
            return await _customerMasterRepository.GetAllCustomerDetails();
        }

        public async Task<CustomerList> EditLoadforCustomerbyId(int id)
        {
            return await _customerMasterRepository.EditLoadforCustomerbyId(id);
        }

    }
}
