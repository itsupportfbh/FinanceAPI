using FinanceApi.Data;
using FinanceApi.Interfaces;
using FinanceApi.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;



namespace FinanceApi.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class UserController : ControllerBase
    {
        private readonly IUserService _service;
        private readonly ApplicationDbContext _context;

        public UserController(IUserService service,ApplicationDbContext applicationDbContext)
        {
            _service = service;
            _context = applicationDbContext;
        }

        [HttpGet("getAll")]
        public async Task<ActionResult<List<User>>> GetAll()
        {
            return Ok(await _service.GetAllAsync());
        }

        [HttpGet("get/{id}")]
        public async Task<ActionResult<User>> GetById(int id)
        {
            var user = await _service.GetByIdAsync(id);
            if (user == null) return NotFound();
            return Ok(user);
        }

        [HttpPost("insert")]
        public async Task<ActionResult<User>> Create(UserDto userDto)
        {
            try
            {
                var created = await _service.CreateAsync(userDto);
                return CreatedAtAction(nameof(GetById), new { id = created.Id }, created);
            }
            catch (ArgumentException ex)
            {
                // Return 400 Bad Request with just the message
                return BadRequest(ex.Message);
            }
            catch (Exception ex)
            {
                // For unexpected exceptions, return 500 Internal Server Error
                return StatusCode(500, "An unexpected error occurred: " + ex.Message);
            }


        }

        [HttpPut("update/{id}")]
        public async Task<ActionResult<User>> Update(int id, UserDto userDto)
        {
            var updated = await _service.UpdateAsync(id, userDto);
            if (updated == null) return NotFound();
            return Ok(updated);
        }

        [HttpDelete("delete/{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            var deleted = await _service.DeleteAsync(id);
            if (!deleted) return NotFound();
            return NoContent();
        }


        [HttpPost("login")]
        public async Task<ActionResult<User>> Login(UserDto userDto)
        {
            try
            {
                var result = await _service.LoginAsync(userDto);

                if (result == null)
                    return Unauthorized("Invalid credentials");

                return Ok(result);
            }
            catch (ArgumentException ex)
            {
                // Return 400 Bad Request with just the message
                return BadRequest(ex.Message);
            }
            catch (Exception ex)
            {
                // For unexpected exceptions, return 500 Internal Server Error
                return StatusCode(500, "An unexpected error occurred: " + ex.Message);
            }


        }

        [HttpPost("changePassword")]
        public async Task<ActionResult<LoginResponseDto>> ChangePassword(ChangePasswordDto changePasswordDto)
        {
            try
            {
                int.TryParse(User.FindFirst("id")?.Value, out int userId);

                var result = await _service.ChangePasswordAsync(userId, changePasswordDto);

                if (!result)
                    return Unauthorized("Current password is incorrect.");

                return Ok(new { message = "Password changed successfully." });
            }
            catch (ArgumentException ex)
            {
                // Return 400 Bad Request with just the message
                return BadRequest(ex.Message);
            }
            catch (Exception ex)
            {
                // For unexpected exceptions, return 500 Internal Server Error
                return StatusCode(500, "An unexpected error occurred: " + ex.Message);
            }


        }

        [HttpPost("forgotPassword")]
        public async Task<IActionResult> ForgotPassword(ForgotPasswordRequestDto request)
        {
            var message = await _service.ForgotPasswordAsync(request);
            if (message == null) return NotFound();
            return Ok(new { message });
        }

        [HttpPost("resetPassword")]
        public async Task<IActionResult> ResetPassword(ResetPasswordDto request)
        {
            var result = await _service.ResetPasswordAsync(request);
            if (!result.IsSuccess)
            {
                return BadRequest(new { message = result.Message });
            }

            return Ok(new { message = result.Message });
        }
        [HttpGet("getAllView")]
        public async Task<ActionResult<List<UserViewDto>>> GetAllView()
        {
            return Ok(await _service.GetAllViewAsync());
        }

        [HttpGet("view/{id}")]
        public async Task<ActionResult<UserViewDto>> View(int id)
        {
            var user = await _service.GetViewByIdAsync(id);
            if (user == null) return NotFound();
            return Ok(user);
        }
        [HttpGet("departments")]
        public IActionResult GetDepartments()
        {
            var list = _context.Department
                .Where(x => x.IsActive)
                .Select(x => new { id = x.Id, name = x.DepartmentName })
                .ToList();

            return Ok(new { data = list });
        }



    }
}
