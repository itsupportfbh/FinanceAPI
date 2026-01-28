// Services/PurchaseAlertService.cs
using FinanceApi.Interfaces;          // repo interface
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;            // DTO


namespace FinanceApi.Services
{
    public class PurchaseAlertService : IPurchaseAlertService
    {
        private readonly IPurchaseAlertRepository _repo;

        public PurchaseAlertService(IPurchaseAlertRepository repo)
        {
            _repo = repo;
        }

        public Task<IEnumerable<PurchaseAlertDTO>> GetUnreadAsync()
            => _repo.GetUnreadAsync();

        public Task MarkReadAsync(int id)
        {
            if (id <= 0) throw new ArgumentOutOfRangeException(nameof(id));
            return _repo.MarkReadAsync(id);
        }

        public Task MarkAllReadAsync()
            => _repo.MarkAllReadAsync();
    }
}
