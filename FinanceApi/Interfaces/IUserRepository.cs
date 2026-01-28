using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IUserRepository
    {
        Task<List<User>> GetAllAsync();
        Task<User?> GetByIdAsync(int id);

        Task<User> CreateAsync(UserDto dto);
        Task<User?> UpdateAsync(int id, UserDto dto);
        Task<bool> DeleteAsync(int id);
        Task<User?> GetByUsernameAsync(string username);
        Task SaveChangesAsync();
        Task<User?> GetUserByEmailAsync(string email);
        Task SaveResetTokenAsync(PasswordResetToken token);
        Task<PasswordResetToken?> GetValidTokenAsync(int userId, string token);
        Task<List<string>> GetRecentPasswordHashesAsync(int userId, int take);
        Task SavePasswordHistoryAsync(int userId, string passwordHash);
        Task<bool> UpdatePasswordAsync(int userId, string hashedPassword);
        Task DeleteTokenAsync(PasswordResetToken token);
        Task<List<UserViewDto>> GetAllViewAsync(int page = 1, int pageSize = 50);
        Task<UserViewDto?> GetViewByIdAsync(int id);
        Task<List<string>> GetRoleNamesAsync(int userId);
        Task<List<int>> GetRoleIdsAsync(int userId);
        Task<List<string>> GetTeamNamesAsync(int userId);

    }
}
