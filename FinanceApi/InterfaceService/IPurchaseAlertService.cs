using FinanceApi.ModelDTO;

namespace FinanceApi.InterfaceService
{
    public interface IPurchaseAlertService
    {
        Task<IEnumerable<PurchaseAlertDTO>> GetUnreadAsync();
        Task MarkReadAsync(int id);
        Task MarkAllReadAsync();
    }
}
