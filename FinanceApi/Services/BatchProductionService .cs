public class BatchProductionService : IBatchProductionService
{
    private readonly IBatchProductionRepository _repo;
    public BatchProductionService(IBatchProductionRepository repo) => _repo = repo;

    public Task<IEnumerable<BatchProductionHeaderDto>> ListAsync(int top = 200) => _repo.ListAsync(top);
    public Task<BatchProductionGetByIdDto> GetByIdAsync(int id) => _repo.GetByIdAsync(id);
    public Task<int> DeleteAsync(int id) => _repo.DeleteAsync(id);

    // ✅ one function
    public Task<int> PostAndSaveAsync(BatchProductionSaveRequest req)
        => _repo.PostAndSaveAsync(req);
}
