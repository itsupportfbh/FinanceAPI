using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IMaterialRequisitionRepository
    {
        Task<IEnumerable<MaterialRequisitionDTO>> GetAllAsync();
        Task<MaterialRequisitionDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(MaterialRequisition mrq);
        Task UpdateAsync(MaterialRequisition mrq);
        Task DeactivateAsync(int id);
    }
}
