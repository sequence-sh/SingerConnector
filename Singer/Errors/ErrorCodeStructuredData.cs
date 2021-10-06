using System.Diagnostics;
using Reductech.EDR.Connectors.StructuredData.Errors;
using Reductech.EDR.Core.Internal.Errors;

namespace Reductech.EDR.Connectors.Singer.Errors
{

/// <summary>
/// Identifying code for an error message in Structured Data
/// </summary>
public sealed record ErrorCodeStructuredData : ErrorCodeBase
{
    private ErrorCodeStructuredData(string code) : base(code) { }

    /// <inheritdoc />
    public override string GetFormatString()
    {
        var localizedMessage =
            ErrorStructuredData_EN.ResourceManager.GetString(Code);

        Debug.Assert(localizedMessage != null, nameof(localizedMessage) + " != null");
        return localizedMessage;
    }

    /*
     * To Generate:
     * Replace ([^\t]+)\t([^\t]+)\t
     * With /// <summary>\r\n/// $2\r\n/// </summary>\r\npublic static readonly ErrorCode $1 = new\(nameof\($1\)\);\r\n
     */

#region Cases

    /// <summary>
    /// Schema Violation: {0}
    /// </summary>
    public static readonly ErrorCodeStructuredData SchemaViolation = new(nameof(SchemaViolation));

#endregion Cases
}

}
