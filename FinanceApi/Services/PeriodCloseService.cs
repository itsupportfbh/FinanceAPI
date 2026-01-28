using FinanceApi.InterfaceService;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;

namespace FinanceApi.Services
{
    public class PeriodCloseService : IPeriodCloseService
    {
        private readonly IPeriodCloseRepository _repository;

        public PeriodCloseService(IPeriodCloseRepository repository)
        {
            _repository = repository;
        }

        public Task<IEnumerable<PeriodOptionDto>> GetPeriodsAsync()
            => _repository.GetPeriodsAsync();

        public Task<PeriodStatusDto?> GetStatusAsync(int periodId)
            => _repository.GetStatusAsync(periodId);

        public Task<PeriodStatusDto> SetLockAsync(int periodId, bool lockFlag, int userId)
            => _repository.SetLockAsync(periodId, lockFlag, userId);

        public Task<int> RunFxRevalAsync(FxRevalRequestDto req, int userId)
            => _repository.RunFxRevalAsync(req, userId);
        public Task EnsureOpenAsync(DateTime transDate)
        {
            return _repository.EnsureOpenAsync(transDate);
        }
        public Task<AccountingPeriod?> GetPeriodByDateAsync(DateTime date)
        {
            return _repository.GetByDateAsync(date);
        }
    }
}
