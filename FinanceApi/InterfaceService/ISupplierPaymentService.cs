using FinanceApi.ModelDTO;

namespace FinanceApi.InterfaceService
{
    public interface ISupplierPaymentService
    {
        Task<IEnumerable<SupplierPaymentDTO>> GetAllAsync();
        Task<IEnumerable<SupplierPaymentDTO>> GetBySupplierAsync(int supplierId);
        Task<bool> CreateAsync(SupplierPaymentCreateDTO dto);
    }
}
