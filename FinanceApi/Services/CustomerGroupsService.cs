using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class CustomerGroupsService : ICustomerGroupsService
    {
        private readonly ICustomerGroupsRepository _repository;

        public CustomerGroupsService(ICustomerGroupsRepository repository)
        {
            _repository = repository; 
        }

        public async Task<IEnumerable<CustomerGroupsDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<CustomerGroupsDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }
        public async Task<int> CreateAsync(CustomerGroups customerGroups)
        {
            return await _repository.CreateAsync(customerGroups);

        }



        public Task UpdateAsync(CustomerGroups customerGroups)
        {
            return _repository.UpdateAsync(customerGroups);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
