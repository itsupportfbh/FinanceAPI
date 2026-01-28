using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class BinServices :IBinServices
    {
        private readonly IBinRepository _repository;

        public BinServices(IBinRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<BinDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(Bin BinDTO)
        {
            return await _repository.CreateAsync(BinDTO);

        }

        public async Task<BinDTO> GetById(long id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(Bin BinDTO)
        {
            return _repository.UpdateAsync(BinDTO);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
