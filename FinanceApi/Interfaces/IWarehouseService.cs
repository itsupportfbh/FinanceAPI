using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IWarehouseService
    {
        Task<IEnumerable<WarehouseDto>> GetAllAsync();
        Task<WarehouseDto> GetByIdAsync(int id);
        Task<int> CreateAsync(Warehouse warehouse);
        Task UpdateAsync(Warehouse warehouse);
        Task DeleteLicense(int id);
        Task<IEnumerable<WarehouseDto>> GetBinNameByIdAsync(int id);

        Task<IEnumerable<WarehouseDto>> GetNameByWarehouseAsync(string name);
    }
}
