using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.InterfaceService
{
    public interface ICatagoryService
    {
        Task<IEnumerable<CatagoryDTO>> GetAllAsync();
        Task<int> CreateAsync(Catagory catagoryDTO);
        Task<CatagoryDTO> GetById(long id);
        Task Update(CatagoryDTO catagoryDTO);
        Task Delete(int id);
    }
}
