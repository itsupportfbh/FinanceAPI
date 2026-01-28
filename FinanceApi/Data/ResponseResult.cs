namespace FinanceApi.Data
{
    public class ResponseResult
    {
        public bool isSuccess {  get; set; }
        public string Message { get; set; }
        public object? Data { get; set; }

        public ResponseResult(bool IsSuccess, string message, object? data)
        {
            isSuccess = IsSuccess;
            Message = message;
            Data = data;
        }
    }

}
