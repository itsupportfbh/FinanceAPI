using Microsoft.Data.SqlClient;
using System.Data;

namespace FinanceApi.Data
{
    public class SqlDbConnectionFactory : IDbConnectionFactory   // 👈 add this
    {
        private readonly string _connectionString;

        public SqlDbConnectionFactory(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        public IDbConnection CreateConnection()
        {
            return new SqlConnection(_connectionString);
        }
    }
}
