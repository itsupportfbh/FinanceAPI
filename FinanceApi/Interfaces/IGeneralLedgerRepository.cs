using FinanceApi.ModelDTO;

namespace FinanceApi.Interfaces
{
    public interface IGeneralLedgerRepository
    {
        Task<IEnumerable<GeneralLedgerDTO>> GetAllAsync();
    }
}
