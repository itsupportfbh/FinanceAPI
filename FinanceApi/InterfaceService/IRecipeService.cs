using FinanceApi.ModelDTO;

namespace FinanceApi.InterfaceService
{
    public interface IRecipeService
    {
        Task<int> CreateAsync(RecipeCreateDto dto, string? createdBy);
        Task<int> UpdateAsync(int id, RecipeUpdateDto dto, string updatedBy);
        Task<RecipeReadDto?> GetByIdAsync(int id);
        Task<IEnumerable<RecipeListDto>> ListAsync();
        Task<bool> DeleteAsync(int id, string deletedBy);
    }
}
