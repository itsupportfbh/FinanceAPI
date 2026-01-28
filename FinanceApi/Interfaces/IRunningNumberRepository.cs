using System.Threading.Tasks;

namespace FinanceApi.Interfaces
{
    public interface IRunningNumberRepository
    {
        Task<int> NextPickingSerialAsync(string dateKey); // returns 1..9999
    }
}