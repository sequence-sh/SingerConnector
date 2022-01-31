using System.IO;
using System.Text.Json;
using Reductech.Sequence.Connectors.Singer.Errors;
using Reductech.Sequence.Core.Steps;

namespace Reductech.Sequence.Connectors.Singer.Tests;

public partial class FromSingerTests : StepTestBase<FromSinger, Array<Entity>>
{
    /// <inheritdoc />
    protected override IEnumerable<StepCase> StepCases
    {
        get
        {
            const string testData1 = @"
{""type"": ""STATE"",  ""value"": {""StateValue"": 1}}
{""type"": ""SCHEMA"", ""stream"": ""test"", ""schema"": {""type"": ""object"", ""additionalProperties"": false, ""properties"": {""a"": {""type"": ""number""}}}, ""key_properties"": [""a""]}
{""type"": ""RECORD"", ""stream"": ""test"", ""record"": {""a"": 1}, ""time_extracted"": ""2021-10-04T15:13:38.301481Z""}
{""type"": ""RECORD"", ""stream"": ""test"", ""record"": {""a"": 2}, ""time_extracted"": ""2021-10-04T15:13:38.301481Z""}
";

            var expectedStatePath = Path.DirectorySeparatorChar + "State.json";
            var step              = IngestAndLogAll(testData1);

            yield return new StepCase(
                    "Read Singer Data",
                    step,
                    Unit.Default,
                    "('a': 1)",
                    "('a': 2)"
                ).WithFileSystem()
                .WithExpectedFileSystem(
                    new[] { (stateOnlyPath: expectedStatePath, "{\"StateValue\": 1}") }
                );

            yield return new StepCase(
                    "Read Singer Data with HandleState",
                    new ForEach<Entity>()
                    {
                        Array = new FromSinger()
                        {
                            Stream = new SCLConstant<StringStream>(testData1.Trim()),
                            HandleState = new LambdaFunction<Entity, Unit>(
                                null,
                                new Log() { Value = new GetAutomaticVariable<Entity>() }
                            )
                        },
                        Action = new LambdaFunction<Entity, Unit>(
                            null,
                            new Log() { Value = new GetAutomaticVariable<Entity>() }
                        ),
                    },
                    Unit.Default,
                    "('StateValue': 1)",
                    "('a': 1)",
                    "('a': 2)"
                ).WithFileSystem()
                //.WithExpectedFileSystem(new[] { (stateOnlyPath: expectedStatePath, "{\"StateValue\": 1}") })
                ;
        }
    }

    public static ForEach<Entity> IngestAndLogAll(string text)
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
            var fromSingerStep = step.Array;

            yield return new ErrorCase(
                "Bad Schema",
                step,
                ErrorCode_Singer.SchemaViolation
                    .ToErrorBuilder("Unknown Violation")
                    .WithLocationSingle(fromSingerStep)
            ).WithFileSystem();

            yield return new ErrorCase(
                "No File System",
                step,
                ErrorCode.MissingContext.ToErrorBuilder("IFileSystem")
                    .WithLocationSingle(step.Array)
            );

            var schemaViolationData = @"
{""type"": ""STATE"",  ""value"": {}}
{""type"": ""SCHEMA"", ""stream"": ""test"", ""schema"": {""type"": ""object"", ""additionalProperties"": false, ""properties"": {""b"": {""type"": ""number""}}}, ""key_properties"": [""b""]}
{""type"": ""RECORD"", ""stream"": ""test"", ""record"": {""a"": 1}, ""time_extracted"": ""2021-10-04T15:13:38.301481Z""}
{""type"": ""RECORD"", ""stream"": ""test"", ""record"": {""a"": ""Some Beautiful Text""}, ""time_extracted"": ""2021-10-04T15:13:38.301481Z""}
";

            var schemaViolationStep = IngestAndLogAll(schemaViolationData);

            yield return new ErrorCase(
                "Data violates schema",
                schemaViolationStep,
                ErrorCode_Singer.SchemaViolation.ToErrorBuilder("Unknown Violation")
                    .WithLocationSingle(schemaViolationStep.Array)
            ).WithFileSystem();

            var malformedData = @"
{""type"": ""STATE"",  ""value"": {}}
{""type"": ""SCHEMA"", ""stream"": ""test"", ""schema"": {""type"": ""object"", ""additionalProperties"": false, ""properties"": {""b"": {""type"": ""number""}}}, ""key_properties"": [""b""]}
{""type"": ""RECORD"", ""stream"": ""test"", ""record"": {""a"": 1, ""time_extracted"": ""2021-10-04T15:13:38.301481Z""}

";

            var malformedDataStep = IngestAndLogAll(malformedData);

            yield return new ErrorCase(
                "Data is malformed",
                malformedDataStep,
                ErrorCode_Singer.JsonParseError.ToErrorBuilder(
                        new JsonException(
                            "Expected depth to be zero at the end of the JSON payload. There is an open JSON object or array that should be closed. Path: $ | LineNumber: 0 | BytePositionInLine: 104."
                        )
                    )
                    .WithLocationSingle(malformedDataStep.Array)
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
