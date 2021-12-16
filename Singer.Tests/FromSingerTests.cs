using System.IO;
using Reductech.EDR.Connectors.Singer.Errors;
using Reductech.EDR.Core.Steps;

namespace Reductech.EDR.Connectors.Singer.Tests;

public partial class FromSingerTests : StepTestBase<FromSinger, Array<Entity>>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            const string testData = @"
{""type"": ""STATE"",  ""value"": {}}
{""type"": ""SCHEMA"", ""stream"": ""test"", ""schema"": {""type"": ""object"", ""additionalProperties"": false, ""properties"": {""a"": {""type"": ""number""}}}, ""key_properties"": [""a""]}
{""type"": ""RECORD"", ""stream"": ""test"", ""record"": {""a"": 1}, ""time_extracted"": ""2021-10-04T15:13:38.301481Z""}
{""type"": ""RECORD"", ""stream"": ""test"", ""record"": {""a"": 2}, ""time_extracted"": ""2021-10-04T15:13:38.301481Z""}
";

            var expectedStatePath = Path.DirectorySeparatorChar + "State.json";
            var step              = IngestAndLogAll(testData);

            yield return new StepCase(
                    "Read Singer Data",
                    step,
                    Unit.Default,
                    "('a': 1)",
                    "('a': 2)"
                ).WithFileSystem()
                .WithExpectedFileSystem(new[] { (stateOnlyPath: expectedStatePath, "{}") });
        }
    }

    public static IStep<Unit> IngestAndLogAll(string text)
    {
        var step = new ForEach<Entity>()
        {
            Array = new FromSinger() { Stream = new SCLConstant<StringStream>(text.Trim()) },
            Action = new LambdaFunction<Entity, Unit>(
                null,
                new Log() { Value = new GetAutomaticVariable<Entity>() }
            )
        };

        return step;
    }

    /// <inheritdoc />
    protected override IEnumerable<ErrorCase> ErrorCases
    {
        get
        {
            foreach (var errorCase in base.ErrorCases)
            {
                if (!errorCase.Name.Contains("HandleState Error"))
                    yield return errorCase;
            }

            const string testDataWithWrongSchema = @"
{""type"": ""STATE"",  ""value"": {}}
{""type"": ""SCHEMA"", ""stream"": ""test"", ""schema"": {""type"": ""object"", ""additionalProperties"": false, ""properties"": {""b"": {""type"": ""number""}}}, ""key_properties"": [""b""]}
{""type"": ""RECORD"", ""stream"": ""test"", ""record"": {""a"": 1}, ""time_extracted"": ""2021-10-04T15:13:38.301481Z""}
{""type"": ""RECORD"", ""stream"": ""test"", ""record"": {""a"": 2}, ""time_extracted"": ""2021-10-04T15:13:38.301481Z""}
";

            var step           = IngestAndLogAll(testDataWithWrongSchema);
            var fromSingerStep = (step as ForEach<Entity>)!.Array;

            yield return new ErrorCase(
                "Bad Schema",
                step,
                ErrorCodeStructuredData.SchemaViolation
                    .ToErrorBuilder("Unknown Violation")
                    .WithLocationSingle(fromSingerStep)
            ).WithFileSystem();
        }
    }

    ///// <inheritdoc />
    //protected override IEnumerable<SerializeCase> SerializeCases
    //{
    //    get
    //    {
    //        yield return new SerializeCase(
    //            "Default Serialization",
    //            new FromSinger() { Stream = new StringConstant("Bar0") },
    //            "FromSinger Stream: \"Bar0\" HandleState: (<> => Log Value: <>)"
    //        );
    //    }
    //}
}
