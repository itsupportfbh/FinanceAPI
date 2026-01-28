using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IUomService
    {
        Task<IEnumerable<UomDTO>> GetAllAsync();
        Task<int> CreateAsync(Uom uom);
        Task<UomDTO> GetById(int id);
        Task UpdateAsync(Uom uom);
        Task DeleteUom(int id);

        Task<UomDTO> GetByName(string name);

        Task<bool> NameExistsAsync(string Name, int excludeId);
    }
}
