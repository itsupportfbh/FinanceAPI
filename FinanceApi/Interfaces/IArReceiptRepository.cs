using System.Collections.Generic;
using System.Threading.Tasks;

namespace UnityWorksERP.Finance.AR
{
    public interface IArReceiptRepository
    {
        Task<IEnumerable<ArReceiptListDto>> GetListAsync();
        Task<ArReceiptDetailDto?> GetByIdAsync(int id);
        Task<IEnumerable<SalesInvoiceOpenDto>> GetOpenInvoicesAsync(int customerId);

        Task<int> CreateAsync(ArReceiptCreateUpdateDto dto, int userId);
        Task UpdateAsync(ArReceiptCreateUpdateDto dto, int userId);
        Task DeleteAsync(int id, int userId);   // soft-delete (IsActive=0)
    }
}

