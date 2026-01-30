using FinanceApi.Models;

public interface IBatchProductionRepository
{
    Task<IEnumerable<BatchProductionHeaderDto>> ListAsync(int top = 200);
    Task<BatchProductionGetByIdDto> GetByIdAsync(int id);
    Task<int> DeleteAsync(int id);

    Task<int> PostAndSaveAsync(BatchProductionSaveRequest req);

    Task<IEnumerable<IngredientExplosionRowDto>> GetIngredientExplosionAsync(int recipeId, int warehouseId, decimal outputQty);
}