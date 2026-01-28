namespace FinanceApi.InterfaceService
{
    public interface IMobileLinkTokenService
    {
        string Generate(string poNo, int minutes = 15);
        bool TryValidate(string token, string poNo, out string error);
    }
}
