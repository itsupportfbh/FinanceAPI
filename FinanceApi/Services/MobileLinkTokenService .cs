using System.Text.Json;
using FinanceApi.InterfaceService;
using Microsoft.AspNetCore.DataProtection;

public class MobileLinkTokenService : IMobileLinkTokenService
{
    private const string Scope = "mobile_receiving";
    private readonly IDataProtector _protector;

    public MobileLinkTokenService(IDataProtectionProvider provider)
    {
        _protector = provider.CreateProtector("UnityERP.MobileReceiving.LinkToken.v1");
    }

    private sealed class Payload
    {
        public string PoNo { get; set; } = "";
        public string Scope { get; set; } = "";
        public DateTime ExpUtc { get; set; }
    }

    public string Generate(string poNo, int minutes = 15)
    {
        var p = new Payload
        {
            PoNo = (poNo ?? "").Trim(),
            Scope = Scope,
            ExpUtc = DateTime.UtcNow.AddMinutes(minutes)
        };

        var json = JsonSerializer.Serialize(p);
        return Uri.EscapeDataString(_protector.Protect(json));
    }

    public bool TryValidate(string token, string poNo, out string error)
    {
        error = "";
        try
        {
            if (string.IsNullOrWhiteSpace(token)) { error = "Token missing"; return false; }

            var raw = Uri.UnescapeDataString(token);
            var json = _protector.Unprotect(raw);
            var p = JsonSerializer.Deserialize<Payload>(json);

            if (p == null) { error = "Bad token"; return false; }
            if (p.Scope != Scope) { error = "Bad scope"; return false; }
            if (!string.Equals(p.PoNo, (poNo ?? "").Trim(), StringComparison.OrdinalIgnoreCase))
            { error = "PO mismatch"; return false; }
            if (DateTime.UtcNow > p.ExpUtc) { error = "Expired"; return false; }

            return true;
        }
        catch { error = "Invalid token"; return false; }
    }
}
