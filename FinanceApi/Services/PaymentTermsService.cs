using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class PaymentTermsService : IPaymentTermsService
    {
        private readonly IPaymentTermsRepository _repository;

        public PaymentTermsService(IPaymentTermsRepository repository)
        {
            _repository = repository;
        }


        public async Task<IEnumerable<PaymentTermsDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(PaymentTerms paymentTermsDTO)
        {
            return await _repository.CreateAsync(paymentTermsDTO);

        }

        public async Task<PaymentTermsDTO> GetById(int id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(PaymentTerms paymentTermsDTO)
        {
            return _repository.UpdateAsync(paymentTermsDTO);
        }


        public async Task DeleteLicense(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
