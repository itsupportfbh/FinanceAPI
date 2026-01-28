using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IPaymentTermsService
    {
        Task<IEnumerable<PaymentTermsDTO>> GetAllAsync();
        Task<int> CreateAsync(PaymentTerms paymentTermsDTO);
        Task<PaymentTermsDTO> GetById(int id);
        Task UpdateAsync(PaymentTerms paymentTermsDTO);
        Task DeleteLicense(int id);
    }
}
