using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class CatagoryServices : ICatagoryService
    {
        private readonly ICatagoryRepository _repository;

        public CatagoryServices(ICatagoryRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<CatagoryDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(Catagory catagoryDTO)
        {
            return await _repository.CreateAsync(catagoryDTO);

        }

        public async Task<CatagoryDTO> GetById(long id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task Update(CatagoryDTO catagoryDTO)
        {
            return _repository.UpdateAsync(catagoryDTO);
        }


        public async Task Delete(int id)
        {
            await _repository.DeactivateAsync(id);
        }
    }
}
