// Services/QuotationService.cs
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;

public class QuotationService : IQuotationService
{
    private readonly IQuotationRepository _repo;
    private readonly IDbConnectionFactory _cf;

    public QuotationService(IQuotationRepository repo, IDbConnectionFactory cf)
    {
        _repo = repo; _cf = cf;
    }

    public Task<IEnumerable<QuotationListDTO>> GetAllAsync() => _repo.GetAllAsync();
    public Task<QuotationListDTO?> GetByIdAsync(int id) => _repo.GetByIdAsync(id);
        
    public async Task<int> CreateAsync(QuotationDTO dto, int userId)
    {
        // Optional: fetch tax rates by Id using a quick Dapper call
       // QuotationCalculator.Compute(dto);
        if (string.IsNullOrWhiteSpace(dto.Number))
            dto.Number = $"QT-{DateTime.UtcNow:yyyyMMdd-HHmmss}";
        return await _repo.CreateAsync(dto, userId);
    }

    public async Task UpdateAsync(QuotationDTO dto, int userId)
    {
        QuotationCalculator.Compute(dto);
        await _repo.UpdateAsync(dto, userId);
    }

    public Task DeleteAsync(int id, int userId) => _repo.DeactivateAsync(id, userId);
}
