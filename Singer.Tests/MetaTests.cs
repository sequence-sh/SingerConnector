using System.Reflection;
using Reductech.EDR.Core.TestHarness;

namespace Reductech.EDR.Connectors.Singer.Tests
{

/// <summary>
/// Makes sure all steps have a test class
/// </summary>
public class MetaTests : MetaTestsBase
{
    /// <inheritdoc />
    public override Assembly StepAssembly => typeof(FromSinger).Assembly;

    /// <inheritdoc />
    public override Assembly TestAssembly => typeof(MetaTests).Assembly;
}

}
