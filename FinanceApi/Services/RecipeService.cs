using FinanceApi.InterfaceService;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;

namespace FinanceApi.Services
{
    public class RecipeService : IRecipeService
    {
        private readonly IRecipeRepository _repo;
        public RecipeService(IRecipeRepository repo)
        {
            _repo = repo;
        }

        public Task<int> CreateAsync(RecipeCreateDto dto, string? createdBy) => _repo.CreateAsync(dto, createdBy);
        public Task<int> UpdateAsync(int id, RecipeUpdateDto dto, string updatedBy) => _repo.UpdateAsync(id, dto, updatedBy);
        public Task<RecipeReadDto?> GetByIdAsync(int id) => _repo.GetByIdAsync(id);
        public Task<IEnumerable<RecipeListDto>> ListAsync() => _repo.ListAsync();
        public Task<bool> DeleteAsync(int id, string deletedBy) => _repo.DeleteAsync(id, deletedBy);
    }
}
