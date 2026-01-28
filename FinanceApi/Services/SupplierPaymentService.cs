using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;

namespace FinanceApi.Services
{
    public class SupplierPaymentService : ISupplierPaymentService
    {
        private readonly ISupplierPaymentRepository _repo;

        public SupplierPaymentService(ISupplierPaymentRepository repo)
        {
            _repo = repo;
        }

        public Task<IEnumerable<SupplierPaymentDTO>> GetAllAsync()
            => _repo.GetAllAsync();

        public Task<IEnumerable<SupplierPaymentDTO>> GetBySupplierAsync(int supplierId)
            => _repo.GetBySupplierAsync(supplierId);

        public Task<bool> CreateAsync(SupplierPaymentCreateDTO dto)
            => _repo.CreateAsync(dto);
    }
}
