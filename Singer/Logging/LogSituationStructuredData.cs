using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Reductech.EDR.Connectors.StructuredData.Logging;
using Reductech.EDR.Core.Internal.Logging;

namespace Reductech.EDR.Connectors.Singer.Logging;

/// <summary>
/// Identifying code for a Core log situation.
/// </summary>
public sealed record LogSituationStructuredData : LogSituationBase
{
    private LogSituationStructuredData(string code, LogLevel logLevel) : base(code, logLevel) { }

    /// <inheritdoc />
    protected override string GetLocalizedString()
    {
        var localizedMessage = LogMessages_EN
            .ResourceManager.GetString(Code);

        Debug.Assert(localizedMessage != null, nameof(localizedMessage) + " != null");
        return localizedMessage;
    }

    /// <summary>
    /// SingerState: {State}
    /// </summary>
    public static readonly LogSituationStructuredData SingerState = new(
        nameof(SingerState),
        LogLevel.Debug
    );
}
