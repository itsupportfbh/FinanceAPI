using FinanceApi.ModelDTO;

namespace FinanceApi.Interfaces
{
    public interface IPurchaseAlertRepository
    {
        Task<IEnumerable<PurchaseAlertDTO>> GetUnreadAsync();
        Task MarkReadAsync(int id);
        Task MarkAllReadAsync();
    }
}
