using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IPaymentTermsRepository
    {
        Task<IEnumerable<PaymentTermsDTO>> GetAllAsync();
        Task<PaymentTermsDTO> GetByIdAsync(int id);
        Task<int> CreateAsync(PaymentTerms paymentTermsDTO);
        Task UpdateAsync(PaymentTerms paymentTermsDTO);
        Task DeactivateAsync(int id);
    }
}
