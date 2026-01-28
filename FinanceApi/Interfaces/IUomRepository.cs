using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IUomRepository
    {
    Task<IEnumerable<UomDTO>> GetAllAsync();
    Task<UomDTO> GetByIdAsync(int id);
    Task<int> CreateAsync(Uom uom);
    Task UpdateAsync(Uom uom);
    Task DeactivateAsync(int id);
  Task<UomDTO> GetByNameAsync(string name);
  Task<bool> NameExistsAsync(string Name, int excludeId);

}
}
