using FinanceApi.Models;

namespace FinanceApi.Interfaces
{
    public interface IUserService
    {
        Task<List<User>> GetAllAsync();
        Task<User?> GetByIdAsync(int id);
        Task<User> CreateAsync(UserDto userDto);
        Task<User?> UpdateAsync(int id, UserDto userDto);
        Task<bool> DeleteAsync(int id);
        Task<LoginResponseDto> LoginAsync(UserDto userDto);
        Task<bool> ChangePasswordAsync(int userId, ChangePasswordDto changePasswordDto);
        Task<string> ForgotPasswordAsync(ForgotPasswordRequestDto request);
        Task<(bool IsSuccess, string Message)> ResetPasswordAsync(ResetPasswordDto request);
        Task<List<UserViewDto>> GetAllViewAsync();
        Task<UserViewDto?> GetViewByIdAsync(int id);
    }
}
