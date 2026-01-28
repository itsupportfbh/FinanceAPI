using FinanceApi.InterfaceService;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;

namespace FinanceApi.Services
{
    public class ProductionPlanService : IProductionPlanService
    {
        private readonly IProductionPlanRepository _repo;
        public ProductionPlanService(IProductionPlanRepository repo)
        {
            _repo = repo;
        }

      
    }
}
