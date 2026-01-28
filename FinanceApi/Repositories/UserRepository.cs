using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Graph.Models;
using System.Net;

namespace FinanceApi.Repositories
{
    using AppUser = FinanceApi.Models.User;
    public class UserRepository : IUserRepository
    {
        private readonly ApplicationDbContext _context;

        public UserRepository(ApplicationDbContext context)
        {
            _context = context;
        }

        public async Task<List<AppUser>> GetAllAsync()
        {
            return await _context.User.Where(c => c.IsActive).OrderBy(c => c.Id).ToListAsync();
        }

        public async Task<AppUser?> GetByIdAsync(int id)
        {
            return await _context.User.FirstOrDefaultAsync(s => s.Id == id && s.IsActive);
        }

        public async Task<AppUser> CreateAsync(UserDto userDto)
        {
            using var tx = await _context.Database.BeginTransactionAsync();

            var user = new AppUser
            {
                Username = userDto.Username,
                Email = userDto.Email,
                PasswordHash = BCrypt.Net.BCrypt.HashPassword(userDto.Password ?? ""),
                CreatedBy = "System",
                CreatedDate = DateTime.UtcNow,
                IsActive = true,
                DepartmentId = userDto.DepartmentId
            };

            _context.User.Add(user);
            await _context.SaveChangesAsync();

            foreach (var levelId in (userDto.ApprovalLevelIds ?? new()).Distinct())
            {
                _context.UserApprovalLevel.Add(new UserApprovalLevel
                {
                    UserId = user.Id,
                    ApprovalLevelId = levelId,
                    CreatedBy = "System",
                    CreatedDate = DateTime.UtcNow
                });
            }

            await _context.SaveChangesAsync();
            await tx.CommitAsync();

            return user;
        }


        public async Task<AppUser?> UpdateAsync(int id, UserDto updatedUser)
        {
            using var tx = await _context.Database.BeginTransactionAsync();

            var existingUser = await _context.User.FirstOrDefaultAsync(s => s.Id == id);
            if (existingUser == null) return null;

            existingUser.Username = updatedUser.Username;
            existingUser.Email = updatedUser.Email;
            existingUser.DepartmentId = updatedUser.DepartmentId;
            if (!string.IsNullOrWhiteSpace(updatedUser.Password))
                existingUser.PasswordHash = BCrypt.Net.BCrypt.HashPassword(updatedUser.Password);

            existingUser.UpdatedBy = "System";
            existingUser.UpdatedDate = DateTime.UtcNow;

            // 🔥 replace mapping rows
            var old = await _context.UserApprovalLevel.Where(x => x.UserId == id).ToListAsync();
            _context.UserApprovalLevel.RemoveRange(old);

            foreach (var levelId in (updatedUser.ApprovalLevelIds ?? new()).Distinct())
            {
                _context.UserApprovalLevel.Add(new UserApprovalLevel
                {
                    UserId = id,
                    ApprovalLevelId = levelId,
                    CreatedBy = "System",
                    CreatedDate = DateTime.UtcNow
                });
            }

            await _context.SaveChangesAsync();
            await tx.CommitAsync();

            return existingUser;
        }



        public async Task<bool> DeleteAsync(int id)
        {
            var user = await _context.User.FirstOrDefaultAsync(s => s.Id == id);
            if (user == null) return false;

            user.IsActive = false;

            await _context.SaveChangesAsync();
            return true;
        }

        public async Task<AppUser?> GetByUsernameAsync(string username)
        {
            return await _context.User.FirstOrDefaultAsync(u => u.Username == username);
        }

        public async Task SaveChangesAsync()
        {
            await _context.SaveChangesAsync();
        }

        public async Task<AppUser?> GetUserByEmailAsync(string email)
        {
            return await _context.User.FirstOrDefaultAsync(u => u.Email == email);
        }

        public async Task SaveResetTokenAsync(PasswordResetToken token)
        {
            _context.PasswordResetToken.Add(token);
            await _context.SaveChangesAsync();
        }

        public async Task<PasswordResetToken?> GetValidTokenAsync(int userId, string token)
        {
            var decodedToken = WebUtility.UrlDecode(token).Replace(" ", "+");

            return await _context.PasswordResetToken
                .FirstOrDefaultAsync(t => t.UserId == userId && t.Token == decodedToken);
        }
        public async Task<List<string>> GetRecentPasswordHashesAsync(int userId, int take)
        {
            return await _context.PasswordHistory
                .Where(p => p.UserId == userId)
                .OrderByDescending(p => p.ChangedAtUtc)
                .Take(take)
                .Select(p => p.PasswordHash)
                .ToListAsync();
        }

        public async Task SavePasswordHistoryAsync(int userId, string newHash)
        {
            var history = new PasswordHistory
            {
                UserId = userId,
                PasswordHash = newHash,
                ChangedAtUtc = DateTime.UtcNow
            };
            _context.PasswordHistory.Add(history);
            await _context.SaveChangesAsync();
        }
        public async Task<bool> UpdatePasswordAsync(int userId, string hashedPassword)
        {
            var user = await _context.User.FirstOrDefaultAsync(u => u.Id == userId);
            if (user == null) return false;

            user.PasswordHash = hashedPassword;
            user.UpdatedDate = DateTime.UtcNow;
            user.UpdatedBy = "System"; // or whoever is resetting

            await _context.SaveChangesAsync();
            return true;
        }
        public async Task DeleteTokenAsync(PasswordResetToken token)
        {
            _context.PasswordResetToken.Remove(token);
            await _context.SaveChangesAsync();
        }
        public async Task<List<UserViewDto>> GetAllViewAsync(int page = 1, int pageSize = 50)
        {
            return await _context.User
                .AsNoTracking()
                .Where(u => u.IsActive)
                .OrderBy(u => u.Id)
                .Skip((page - 1) * pageSize)
                .Take(pageSize)
                .Select(u => new UserViewDto
                {
                    Id = u.Id,
                    Username = u.Username,
                    Email = u.Email ?? "",
                    IsActive = u.IsActive,

                    // ✅ Approval Roles
                    ApprovalLevelNames = _context.UserApprovalLevel
                        .Where(x => x.UserId == u.Id)
                        .Join(_context.ApprovalLevel,
                              x => x.ApprovalLevelId,
                              r => r.Id,
                              (x, r) => r.Name)
                        .ToList(),

                    // ✅ Teams (DepartmentName)
                    Teams = _context.User
                        .Where(t => t.Id == u.Id)
                        .Join(_context.Department,
                              t => t.DepartmentId,
                              d => d.Id,
                              (t, d) => d.DepartmentName)
                        .ToList()
                })
                .ToListAsync();
        }


        public async Task<UserViewDto?> GetViewByIdAsync(int id)
        {
            return await _context.User
                .Where(u => u.Id == id && u.IsActive)  // ✅ bool
                .Select(u => new UserViewDto
                {
                    Id = u.Id,
                    Username = u.Username,
                    Email = u.Email ?? "",
                    IsActive = u.IsActive,
                    DepartmentId = u.DepartmentId,


                    ApprovalLevelIds = _context.UserApprovalLevel
                        .Where(x => x.UserId == u.Id)
                        .Select(x => x.ApprovalLevelId)
                        .ToList(),

                    ApprovalLevelNames =
                        (from ual in _context.UserApprovalLevel
                         join al in _context.ApprovalLevel on ual.ApprovalLevelId equals al.Id
                         where ual.UserId == u.Id && al.IsActive == true
                         select al.Name).ToList()
                })
                .FirstOrDefaultAsync();
        }

        public async Task<List<int>> GetRoleIdsAsync(int userId)
        {
            return await _context.UserApprovalLevel
                .Where(x => x.UserId == userId)
                .Select(x => x.ApprovalLevelId)
                .Distinct()
                .ToListAsync();
        }

        public async Task<List<string>> GetRoleNamesAsync(int userId)
        {
            return await (
                from ual in _context.UserApprovalLevel
                join al in _context.ApprovalLevel on ual.ApprovalLevelId equals al.Id
                where ual.UserId == userId && al.IsActive == true

                select al.Name
            ).Distinct().ToListAsync();
        }
        public async Task<List<string>> GetTeamNamesAsync(int userId)
        {
            // single department team (quick)
            var team = await _context.User
                .Where(u => u.Id == userId)
                .Join(_context.Department,
                      u => u.DepartmentId,
                      d => d.Id,
                      (u, d) => d.DepartmentName)
                .FirstOrDefaultAsync();

            return string.IsNullOrWhiteSpace(team) ? new List<string>() : new List<string> { team };
        }


    }
}
