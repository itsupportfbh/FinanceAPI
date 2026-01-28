using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Services
{
    public class CreditNoteService : ICreditNoteService
    {
        private readonly ICreditNoteRepository _repo;

        public CreditNoteService(ICreditNoteRepository repo)
        {
            _repo = repo;
        }

        public Task<IEnumerable<CreditNoteDTO>> GetAllAsync() => _repo.GetAllAsync();

        public Task<CreditNoteDTO?> GetByIdAsync(int id) => _repo.GetByIdAsync(id);

        public Task<int> CreateAsync(CreditNote cn)
        {
            if (cn.Lines is null || cn.Lines.Count == 0)
                throw new ArgumentException("At least one line is required.");
            return _repo.CreateAsync(cn);
        }

        public Task UpdateAsync(CreditNote cn) => _repo.UpdateAsync(cn);

        public Task DeactivateAsync(int id, int updatedBy) => _repo.DeactivateAsync(id, updatedBy);

        public Task<IEnumerable<object>> GetDoLinesAsync(int doId, int? excludeCnId = null)
       => _repo.GetDoLinesAsync(doId, excludeCnId);
    }
}
