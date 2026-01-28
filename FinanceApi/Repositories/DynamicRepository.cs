using FinanceApi.Data;
using System.Data;

namespace FinanceApi.Repositories
{
    public abstract class DynamicRepository
    {
        private readonly IDbConnectionFactory _connectionFactory;

        protected DynamicRepository(IDbConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;
        }

        // Helper property to create/open connection
        protected IDbConnection Connection => _connectionFactory.CreateConnection();
    }
}
