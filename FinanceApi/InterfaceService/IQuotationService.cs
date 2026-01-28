using FinanceApi.ModelDTO;

namespace FinanceApi.InterfaceService
{
    public interface IQuotationService
    {
        Task<IEnumerable<QuotationListDTO>> GetAllAsync();
        Task<QuotationListDTO?> GetByIdAsync(int id);
        Task<int> CreateAsync(QuotationDTO dto, int userId);
        Task UpdateAsync(QuotationDTO dto, int userId);
        Task DeleteAsync(int id, int userId);
    }
}
