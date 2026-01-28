// Services/SupplierDebitNoteService.cs
using FinanceApi.InterfaceService;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class SupplierDebitNoteService : ISupplierDebitNoteService
    {
        private readonly ISupplierDebitNoteRepository _repo;

        public SupplierDebitNoteService(ISupplierDebitNoteRepository repo)
        {
            _repo = repo;
        }

        public Task<IEnumerable<SupplierDebitNoteDTO>> GetAllAsync()
            => _repo.GetAllAsync();

        public Task<SupplierDebitNoteDTO?> GetByIdAsync(int id)
            => _repo.GetByIdAsync(id);

        public Task<int> CreateAsync(SupplierDebitNote model)
            => _repo.CreateAsync(model);

        public Task UpdateAsync(SupplierDebitNote model)
            => _repo.UpdateAsync(model);

        public Task DeleteAsync(int id)
            => _repo.DeleteAsync(id);
        // Services/SupplierInvoicePinService.cs
        public Task<dynamic?> GetDebitNoteSourceAsync(int pinId)
            => _repo.GetDebitNoteSourceAsync(pinId);
        public Task MarkDebitNoteAsync(int pinId, string userName)
       => _repo.MarkDebitNoteAsync(pinId, userName);

    }
}
