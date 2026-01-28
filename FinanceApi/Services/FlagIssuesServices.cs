using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class FlagIssuesServices : IflagIssuesServices
    {
        private readonly IFlagIssuesRepository _repository;

        public FlagIssuesServices(IFlagIssuesRepository repository)
        {
            _repository = repository;
        }

        public async Task<IEnumerable<FlagIssuesDTO>> GetAllAsync()
        {
            return await _repository.GetAllAsync();
        }

        public async Task<int> CreateAsync(FlagIssues flagIssuesDTO)
        {
            return await _repository.CreateAsync(flagIssuesDTO);

        }

        public async Task<FlagIssuesDTO> GetById(long id)
        {
            return await _repository.GetByIdAsync(id);
        }

        public Task UpdateAsync(FlagIssues flagIssuesDTO)
        {
            return _repository.UpdateAsync(flagIssuesDTO);
        }


        public async Task DeleteAsync(int id)
        {
            await _repository.DeactivateAsync(id);
        }

        public async Task<FlagIssuesDTO> GetByName(string name)
        {
            return await _repository.GetByNameAsync(name);
        }

        public async Task<bool> NameExistsAsync(string Name, long excludeId)
        {
            return await _repository.NameExistsAsync(Name, excludeId);
        }


    }
}
