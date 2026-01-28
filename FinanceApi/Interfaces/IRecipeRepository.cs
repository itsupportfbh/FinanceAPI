using FinanceApi.ModelDTO;

namespace FinanceApi.Interfaces
{
    public interface IRecipeRepository
    {
        Task<int> CreateAsync(RecipeCreateDto dto, string? createdBy);
        Task<int> UpdateAsync(int recipeId, RecipeUpdateDto dto, string updatedBy);

        Task<RecipeReadDto?> GetByIdAsync(int id);
        Task<IEnumerable<RecipeListDto>> ListAsync();

        // "delete" -> status change (soft delete)
        Task<bool> DeleteAsync(int id, string deletedBy);
    }
}
