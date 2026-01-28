using FinanceApi.ModelDTO;

namespace FinanceApi.Interfaces
{
    public interface IQuotationRepository
    {
        Task<int> CreateAsync(QuotationDTO dto, int userId);
        Task UpdateAsync(QuotationDTO dto, int userId);
        Task<QuotationListDTO?> GetByIdAsync(int id);
        Task<IEnumerable<QuotationListDTO>> GetAllAsync();
        Task DeactivateAsync(int id, int userId);
    }
}
