using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace FinanceApi.Swagger
{
    public class FileUploadOperationFilter : IOperationFilter
    {
        public void Apply(OpenApiOperation operation, OperationFilterContext context)
        {
            var hasForm = context.ApiDescription.ParameterDescriptions
                .Any(p => p.Source?.Id == "Form");

            if (!hasForm) return;

            operation.RequestBody = new OpenApiRequestBody
            {
                Content =
            {
                ["multipart/form-data"] = new OpenApiMediaType
                {
                    Schema = new OpenApiSchema
                    {
                        Type = "object",
                        Properties =
                        {
                            ["file"] = new OpenApiSchema { Type = "string", Format = "binary" },
                            ["lang"] = new OpenApiSchema { Type = "string" },
                            ["module"] = new OpenApiSchema { Type = "string" },
                            ["refNo"] = new OpenApiSchema { Type = "string" },
                            ["createdBy"] = new OpenApiSchema { Type = "string" },
                            ["currencyId"] = new OpenApiSchema { Type = "integer", Format = "int32" }
                        }
                    }
                }
            }
            };
        }
    }
}
