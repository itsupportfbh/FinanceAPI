using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class BankService : IBankService
    {
        private readonly IBankRepository _bankRepository;

        public BankService(IBankRepository bankRepository)
        {
            _bankRepository = bankRepository;
        }

        public async Task<IEnumerable<BankDto>> GetAllAsync()
        {
            return await _bankRepository.GetAllAsync();
        }

        public async Task<int> CreateAsync(Bank BankDto)
        {
            return await _bankRepository.CreateAsync(BankDto);

        }

        public async Task<BankDto> GetById(long id)
        {
            return await _bankRepository.GetByIdAsync(id);
        }

        public Task UpdateAsync(Bank BankDto)
        {
            return _bankRepository.UpdateAsync(BankDto);
        }


        public async Task DeleteAsync(int id)
        {
            await _bankRepository.DeactivateAsync(id);
        }
    }
}
