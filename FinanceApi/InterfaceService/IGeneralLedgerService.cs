using FinanceApi.ModelDTO;

namespace FinanceApi.InterfaceService
{
    public interface IGeneralLedgerService
    {
        Task<IEnumerable<GeneralLedgerDTO>> GetAllAsync();
    }
}
