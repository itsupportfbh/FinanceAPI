using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface IMaterialRequisitionService
    {
        Task<IEnumerable<MaterialRequisitionDTO>> GetAllAsync();
        Task<MaterialRequisitionDTO> GetById(int id);
        Task<int> CreateAsync(MaterialRequisition mrq);
        Task UpdateAsync(MaterialRequisition mrq);
        Task DeleteAsync(int id);
    }
}
