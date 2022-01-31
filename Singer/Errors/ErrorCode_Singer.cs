using System.Diagnostics;
using Reductech.Sequence.Core.Internal.Errors;

namespace Reductech.Sequence.Connectors.Singer.Errors;

/// <summary>
/// Identifying code for an error message in Structured Data
/// </summary>
public sealed record ErrorCode_Singer : ErrorCodeBase
{
    private ErrorCode_Singer(string code) : base(code) { }

    /// <inheritdoc />
    public override string GetFormatString()
    {
        var localizedMessage = ErrorSinger_EN.ResourceManager.GetString(Code);

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
    public static readonly ErrorCode_Singer SchemaViolation = new(nameof(SchemaViolation));

    /// <summary>
    /// Json Parse Error: {0}
    /// </summary>
    public static readonly ErrorCode_Singer JsonParseError = new(nameof(JsonParseError));

#endregion Cases
}
