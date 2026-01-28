using FinanceApi.ModelDTO;

namespace FinanceApi.Interfaces
{
    public interface IGstF5Repository
    {
        Task<IEnumerable<GstFinancialYearOptionDto>> GetFinancialYearsAsync();
        Task<IEnumerable<GstPeriodOptionDto>> GetPeriodsByYearAsync(int fyStartYear);

        Task<GstReturnDto> GetReturnForPeriodAsync(int periodId, int userId);
        Task<GstReturnDto> ApplyAndLockAsync(GstApplyLockRequest req, int userId);

        Task<IEnumerable<GstAdjustmentDto>> GetAdjustmentsAsync(int periodId);
        Task<GstAdjustmentDto> SaveAdjustmentAsync(GstAdjustmentDto dto, int userId);
        Task DeleteAdjustmentAsync(int id, int userId);

        Task<IEnumerable<GstDocRowDto>> GetDocsByPeriodAsync(int periodId);
        Task<IEnumerable<GstDetailRowDto>> GetGstDetailsAsync(
    DateTime startDate,
    DateTime endDate,
    string? docType,
    string? partySearch);

    }
}
