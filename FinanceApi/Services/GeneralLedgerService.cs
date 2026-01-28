using FinanceApi.Interfaces;
using FinanceApi.InterfaceService;
using FinanceApi.ModelDTO;

namespace FinanceApi.Services
{
    public class GeneralLedgerService : IGeneralLedgerService
    {
        private readonly IGeneralLedgerRepository _generalRepository;

        public GeneralLedgerService(IGeneralLedgerRepository generalRepository)
        {
            _generalRepository = generalRepository;
        }


        public async Task<IEnumerable<GeneralLedgerDTO>> GetAllAsync()
        {
            return await _generalRepository.GetAllAsync();
        }
    }
}
