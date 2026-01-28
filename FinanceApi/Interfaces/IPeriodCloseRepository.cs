using FinanceApi.ModelDTO;

namespace FinanceApi.Interfaces
{
    public interface IPeriodCloseRepository
    {
        Task<IEnumerable<PeriodOptionDto>> GetPeriodsAsync();
        Task<PeriodStatusDto?> GetStatusAsync(int periodId);
        Task<PeriodStatusDto> SetLockAsync(int periodId, bool lockFlag, int userId);
        Task<int> RunFxRevalAsync(FxRevalRequestDto req, int userId);
        Task EnsureOpenAsync(DateTime transDate);
        Task<AccountingPeriod?> GetByDateAsync(DateTime date);

    }
}
