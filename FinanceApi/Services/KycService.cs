using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using FinanceApi.Repositories;

namespace FinanceApi.Services
{
    public class KycService : IKycService
    {
        private readonly IKYCRepository _kycRepository;

        public KycService(IKYCRepository kycRepository)
        {
            _kycRepository = kycRepository;
        }

        public async Task<IEnumerable<KYCDTO>> GetAllAsync()
        {
            return await _kycRepository.GetAllAsync();
        }

        public async Task<int> CreateAsync(KYC kyc)
        {
            return await _kycRepository.CreateAsync(kyc);

        }

        public async Task<KYCDTO> GetById(int id)
        {
            return await _kycRepository.GetByIdAsync(id);
        }

        public Task UpdateAsync(KYC kyc)
        {
            return _kycRepository.UpdateAsync(kyc);
        }


        public async Task DeleteAsync(int id)
        {
            await _kycRepository.DeactivateAsync(id);
        }
    }
}