using System.Data;

namespace FinanceApi.Data
{
    public interface IDbConnectionFactory
    {
        IDbConnection CreateConnection();
    }
}
