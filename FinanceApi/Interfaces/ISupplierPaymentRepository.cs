using FinanceApi.ModelDTO;

namespace FinanceApi.Interfaces
{
    public interface ISupplierPaymentRepository
    {
        Task<IEnumerable<SupplierPaymentDTO>> GetAllAsync();
        Task<IEnumerable<SupplierPaymentDTO>> GetBySupplierAsync(int supplierId);
        Task<bool> CreateAsync(SupplierPaymentCreateDTO dto);
    }
}
