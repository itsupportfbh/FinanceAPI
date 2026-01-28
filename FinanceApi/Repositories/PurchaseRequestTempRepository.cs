using Dapper;
using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.ModelDTO;
using FinanceApi.Models;
using System.Data;
using System.Transactions;

namespace FinanceApi.Repositories
{
    public class PurchaseRequestTempRepository : DynamicRepository, IPurchaseRequestTempRepository
    {
        private readonly ApplicationDbContext _context;

        public PurchaseRequestTempRepository(IDbConnectionFactory connectionFactory)
          : base(connectionFactory)
        {
        }

        public async Task<int> CreateAsync(PurchaseRequestTemp t, IDbTransaction tx = null)
        {
            const string sql = @"
INSERT INTO PurchaseRequestTemp
(Requester,DepartmentID,DeliveryDate,MultiLoc,Oversea,PRLines,Description,Status,IsActive,CreatedBy,CreatedDate,UpdatedBy,UpdatedDate)
OUTPUT INSERTED.Id
VALUES
(@Requester,@DepartmentID,@DeliveryDate,@MultiLoc,@Oversea,@PRLines,@Description,@Status,@IsActive,@CreatedBy,SYSUTCDATETIME(),@UpdatedBy,SYSUTCDATETIME())";
            return await Connection.ExecuteScalarAsync<int>(sql, t, tx);
        }

        public async Task UpdateAsync(PurchaseRequestTemp t, IDbTransaction tx = null)
        {
            const string sql = @"
UPDATE PurchaseRequestTemp
SET Requester=@Requester,
    DepartmentID=@DepartmentID,
    DeliveryDate=@DeliveryDate,
    MultiLoc=@MultiLoc,
    Oversea=@Oversea,
    PRLines=@PRLines,
    Description=@Description,
    Status=@Status,
    UpdatedBy=@UpdatedBy,
    UpdatedDate=SYSUTCDATETIME()
WHERE Id=@Id AND IsActive=1";
            await Connection.ExecuteAsync(sql, t, tx);
        }

        public async Task<PurchaseRequestTemp> GetByIdAsync(int id)
        {
            const string sql = @"SELECT * FROM PurchaseRequestTemp WHERE Id=@id AND IsActive=1";
            return await Connection.QueryFirstOrDefaultAsync<PurchaseRequestTemp>(sql, new { id });
        }


        public async Task<IEnumerable<PurchaseRequestTempDto>> ListAsync(int? departmentId = null)
        {
            var sql = @"
SELECT  t.*,
        d.DepartmentName
FROM    PurchaseRequestTemp t
LEFT JOIN Department d ON d.Id = t.DepartmentID
WHERE   t.IsActive = 1
" + (departmentId.HasValue ? " AND t.DepartmentID = @departmentId" : "") + @"
ORDER BY t.Id DESC";

            return await Connection.QueryAsync<PurchaseRequestTempDto>(sql, new { departmentId });
        }

        public async Task DeleteAsync(int id, string userId)
        {
            const string sql = @"UPDATE PurchaseRequestTemp SET IsActive=0, UpdatedBy=@userId, UpdatedDate=SYSUTCDATETIME() WHERE Id=@id";
            await Connection.ExecuteAsync(sql, new { id, userId });
        }

        public async Task<int> PromoteAsync(int tempId, string userId)
        {
            // Ambient transaction; no Connection.Open/Close anywhere
            using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

            // 1) Load draft
            var temp = await Connection.QueryFirstOrDefaultAsync<PurchaseRequestTemp>(
                "SELECT * FROM PurchaseRequestTemp WHERE Id=@tempId AND IsActive=1",
                new { tempId });

            if (temp == null) throw new InvalidOperationException("Draft not found.");

            // 2) Generate next PR number
            const string getLastPurchaseRequestNo = @"
SELECT TOP 1 PurchaseRequestNo
FROM PurchaseRequest
WHERE ISNUMERIC(SUBSTRING(PurchaseRequestNo, 4, LEN(PurchaseRequestNo))) = 1
ORDER BY Id DESC";

            var lastPR = await Connection.QueryFirstOrDefaultAsync<string>(getLastPurchaseRequestNo);
            int nextNumber = 1;
            if (!string.IsNullOrWhiteSpace(lastPR) && lastPR.StartsWith("PR-"))
            {
                var numericPart = lastPR.Substring(3);
                if (int.TryParse(numericPart, out var lastNumber)) nextNumber = lastNumber + 1;
            }
            var newPRNumber = $"PR-{nextNumber:D4}";

            // 3) Insert into real PurchaseRequest
            const string insertPR = @"
INSERT INTO PurchaseRequest
    (Requester, DepartmentID, DeliveryDate, MultiLoc, Oversea, PRLines, CreatedDate,
     UpdatedDate, CreatedBy, UpdatedBy, Description, PurchaseRequestNo, IsActive, Status)
OUTPUT INSERTED.Id
VALUES
    (@Requester, @DepartmentID, @DeliveryDate, @MultiLoc, @Oversea, @PRLines, SYSUTCDATETIME(),
     SYSUTCDATETIME(), @CreatedBy, @UpdatedBy, @Description, @PurchaseRequestNo, 1, 1);";

            var newId = await Connection.ExecuteScalarAsync<int>(insertPR, new
            {
                Requester = temp.Requester,
                DepartmentID = temp.DepartmentID,
                DeliveryDate = temp.DeliveryDate,
                MultiLoc = temp.MultiLoc,
                Oversea = temp.Oversea,
                PRLines = temp.PRLines,
                CreatedBy = temp.CreatedBy,
                UpdatedBy = userId,
                Description = temp.Description,
                PurchaseRequestNo = newPRNumber
            });

            // 4) Mark draft inactive
            await Connection.ExecuteAsync(
                "UPDATE PurchaseRequestTemp SET IsActive=0, UpdatedBy=@userId, UpdatedDate=SYSUTCDATETIME() WHERE Id=@tempId",
                new { userId, tempId });

            // Commit ambient transaction
            scope.Complete();
            return newId;
        }
    }

}
