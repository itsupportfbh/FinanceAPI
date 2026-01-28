using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IKycService 
    {
        Task<IEnumerable<KYCDTO>> GetAllAsync();
        Task<int> CreateAsync(KYC kyc);
        Task<KYCDTO> GetById(int id);
        Task UpdateAsync(KYC kyc);
        Task DeleteAsync(int id);
    }
}
