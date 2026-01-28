using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IKYCRepository
    {
        Task<IEnumerable<KYCDTO>> GetAllAsync();
        Task<KYCDTO> GetByIdAsync(int id);

        Task<int> CreateAsync(KYC kyc);

        Task UpdateAsync(KYC kyc);
        Task DeactivateAsync(int id);
    }
}
