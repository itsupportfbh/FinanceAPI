using FinanceApi.ModelDTO;
using System.Threading.Tasks;

namespace FinanceApi.Interfaces
{
    public interface IIncotermsRepository
    {
        Task<IEnumerable<IncotermsDTO>> GetAllAsync();
        Task<IncotermsDTO> GetByIdAsync(long id);

        Task<int> CreateAsync(IncotermsDTO incotermsDTO);
        Task DeactivateAsync(int id);
        Task UpdateAsync(IncotermsDTO incotermsDTO);

        Task<bool> NameExistsAsync(string IncotermsName, long excludeId);

        Task<IncotermsDTO> GetByNameAsync(string name);
    }
}
