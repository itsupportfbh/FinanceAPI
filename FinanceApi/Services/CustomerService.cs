using FinanceApi.Interfaces;
using FinanceApi.Models;


namespace FinanceApi.Services
{
    public class CustomerService : ICustomerService
    {
        private readonly ICustomerRepository _repository;
         
        public CustomerService(ICustomerRepository repository)
        {
            _repository = repository;
        }

        public async Task<List<CustomerDto>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<CustomerDto?> GetByIdAsync(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public async Task<Customer> CreateAsync(Customer customer)
        {
            return await _repository.CreateAsync(customer);
        }

        public async Task<Customer?> UpdateAsync(int id,Customer customer)
        {
            return await _repository.UpdateAsync(id, customer);
        }

        public async Task<bool> DeleteAsync(int id)
        {
            return await _repository.DeleteAsync(id);
        }

    }
}
