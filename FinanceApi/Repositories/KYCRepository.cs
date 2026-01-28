using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;

namespace FinanceApi.Repositories
{
    public class KYCRepository : DynamicRepository,IKYCRepository
    {
        public KYCRepository(IDbConnectionFactory connectionFactory)
      : base(connectionFactory)
        {
        }


        public async Task<IEnumerable<KYCDTO>> GetAllAsync()
        {
            const string query = @"
                SELECT * from KYC";

            return await Connection.QueryAsync<KYCDTO>(query);
        }


        public async Task<KYCDTO> GetByIdAsync(int id)
        {

            const string query = "SELECT * FROM KYC WHERE Id = @Id";

            return await Connection.QuerySingleAsync<KYCDTO>(query, new { Id = id });
        }

        public async Task<int> CreateAsync(KYC kyc)
        {
            const string query = @"
INSERT INTO KYC 
 (DLImage, UtilityBillImage, BSImage, ACRAImage,
  DLImageName, UtilityBillImageName, BSImageName, ACRAImageName,
  CreatedDate, CreatedBy, UpdatedDate, UpdatedBy,
  ApprovedBy, IsApproved,IsActive)
OUTPUT INSERTED.Id
VALUES
 (@DLImage, @UtilityBillImage, @BSImage, @ACRAImage,
  @DLImageName, @UtilityBillImageName, @BSImageName, @ACRAImageName,
  @CreatedDate, @CreatedBy, @UpdatedDate, @UpdatedBy,
  @ApprovedBy, @IsApproved,@IsActive)";

            return await Connection.QueryFirstAsync<int>(query, kyc);
        }



        public async Task UpdateAsync(KYC kyc)
        {
            const string query = "UPDATE KYC SET DLImage = @DLImage, UtilityBillImage =@UtilityBillImage, BSImageName =@BSImageName WHERE Id = @Id";
            await Connection.ExecuteAsync(query, kyc);
        }

        public async Task DeactivateAsync(int id)
        {
            const string query = "UPDATE KYC SET IsActive = 0 WHERE ID = @id";
            await Connection.ExecuteAsync(query, new { ID = id });
        }
    }
}
