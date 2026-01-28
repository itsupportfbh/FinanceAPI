using FinanceApi.Data;
using System.Data;

namespace FinanceApi.Repositories
{
    public abstract class BaseRepository
    {
        private readonly IDbConnectionFactory _connectionFactory;
        protected BaseRepository(IDbConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }
        protected IDbConnection CreateConnection()
        {
            return _connectionFactory.CreateConnection();
        }
    }
}
