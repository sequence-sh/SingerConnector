using System.IO.Abstractions;
using System.Linq;
using FluentAssertions;
using Xunit;

namespace Sequence.Connectors.Singer.Tests;

public class ConnectorInjectionTest
{
    [Fact]
    public void TestConnectorInjection()
    {
        var ci       = new ConnectorInjection();
        var contexts = ci.TryGetInjectedContexts();
        contexts.ShouldBeSuccessful();

        contexts.Value.Should().HaveCount(1);

        var pair = contexts.Value.Single();
        pair.Name.Should().Be("Singer.FileSystem");
        pair.Context.Should().BeOfType<FileSystem>();
    }
}
