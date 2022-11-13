﻿using System.Collections.Immutable;
using System.IO;
using System.IO.Abstractions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using Json.Schema;
using Sequence.Connectors.Singer.Errors;
using Sequence.Core.Enums;
using Sequence.Core.Internal.Errors;
using Entity = Sequence.Core.Entity;

namespace Sequence.Connectors.Singer;

/// <summary>
/// Extracts the data from a Singer Tap and converts it to entities
/// </summary>
public sealed class FromSinger : CompoundStep<Array<Entity>>
{
    /// <inheritdoc />
    protected override async ValueTask<Result<Array<Entity>, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var ss = await Stream.Run(stateMonad, cancellationToken);

        if (ss.IsFailure)
        {
            return ss.ConvertFailure<Array<Entity>>();
        }

        var asyncEnumerable = ReadSingerStreamEntities(
            ss.Value,
            this,
            stateMonad,
            HandleState,
            cancellationToken
        );

        var lazyArray = new LazyArray<Entity>(asyncEnumerable);

        return lazyArray;
    }

    /// <summary>
    /// Stream containing the Json data.
    /// </summary>
    [StepProperty(1)]
    [Required]
    public IStep<StringStream> Stream { get; set; } = null!;

    /// <summary>
    /// How to handle the state
    /// </summary>
    [FunctionProperty(2)]
    [DefaultValueExplanation("Writes the state to a file called State.Json")]
    public LambdaFunction<Entity, Unit>? HandleState { get; set; } = null;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<FromSinger, Array<Entity>>();

    private static async IAsyncEnumerable<Entity> ReadSingerStreamEntities(
        StringStream stringStream,
        IStep step,
        IStateMonad stateMonad,
        LambdaFunction<Entity, Unit>? handleState,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Dictionary<string, JsonSchema> schemaDict   = new();
        var                            currentState = stateMonad.GetState().ToImmutableDictionary();

        await foreach (var result in ReadSingerStream(stringStream, cancellationToken))
        {
            if (result.IsFailure)
                throw new ErrorException(result.Error.WithLocation(step));

            if (result.Value is SingerSchema singerSchema)
            {
                schemaDict[singerSchema.Stream] = singerSchema.Schema;
            }
            else if (result.Value is SingerRecord singerRecord)
            {
                if (schemaDict.TryGetValue(singerRecord.Stream, out var schema))
                {
                    var    validationResult = schema.Validate(singerRecord.Record);
                    string message          = validationResult.Message ?? "Unknown Violation";

                    if (!validationResult.IsValid)
                        throw new ErrorException(
                            ErrorCode_Singer.SchemaViolation
                                .ToErrorBuilder(message)
                                .WithLocationSingle(step)
                        );
                }

                yield return Entity.Create(singerRecord.Record);
            }
            else if (result.Value is SingerState singerState)
            {
                if (handleState is not null)
                {
                    var stateEntity = Entity.Create(singerState.Value);

                    var scopedMonad = new ScopedStateMonad(
                        stateMonad,
                        currentState,
                        handleState.VariableNameOrItem,
                        new KeyValuePair<VariableName, ISCLObject>(
                            handleState.VariableNameOrItem,
                            stateEntity
                        )
                    );

                    var handleStateResult =
                        await handleState.StepTyped.Run(scopedMonad, cancellationToken);

                    if (handleStateResult.IsFailure)
                        throw new ErrorException(handleStateResult.Error);
                }
                else
                {
                    var fileSystemResult =
                        stateMonad.ExternalContext.TryGetContext<IFileSystem>(
                            ConnectorInjection.FileSystemKey
                        );

                    if (fileSystemResult.IsFailure)
                        throw new ErrorException(fileSystemResult.Error.WithLocation(step));

                    var fullPath = fileSystemResult.Value.Path.Combine(
                        fileSystemResult.Value.Directory.GetCurrentDirectory(),
                        "State.json"
                    );

                    var stateJson = singerState.Value.ToString() ?? "";

                    var writeFileResult = await WriteFileAsync(
                        fileSystemResult.Value,
                        fullPath,
                        stateJson,
                        cancellationToken
                    );

                    if (writeFileResult.IsFailure)
                        throw new ErrorException(writeFileResult.Error.WithLocation(step));
                }
            }
        }
    }

    private static async Task<Result<Unit, IErrorBuilder>> WriteFileAsync(
        IFileSystem fileSystem,
        string path,
        string contents,
        CancellationToken cancellationToken)
    {
        Maybe<IErrorBuilder> error;

        try
        {
            await fileSystem.File.WriteAllTextAsync(path, contents, cancellationToken);
            error = Maybe<IErrorBuilder>.None;
        }
        catch (Exception e)
        {
            error = Maybe<IErrorBuilder>.From(
                ErrorCode.ExternalProcessError.ToErrorBuilder(e.Message)
            );
        }

        if (error.HasValue)
            return Result.Failure<Unit, IErrorBuilder>(error.Value);

        return Unit.Default;
    }

    private static async IAsyncEnumerable<Result<SingerObject, IErrorBuilder>> ReadSingerStream(
        StringStream stringStream,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var (stream, encoding) = stringStream.GetStream();

        var options = new JsonSerializerOptions() { Converters = { SingerJsonConverter.Instance } };

        var streamReader = new StreamReader(stream, encoding.Convert());

        while (!streamReader.EndOfStream && !cancellationToken.IsCancellationRequested)
        {
            var line = await streamReader.ReadLineAsync();

            if (!string.IsNullOrWhiteSpace(line))
            {
                Result<SingerObject, IErrorBuilder> result;

                try
                {
                    var obj = JsonSerializer.Deserialize<SingerObject>(line, options);

                    if (obj is null)
                        result = ErrorCode.CouldNotParse.ToErrorBuilder(line, nameof(SingerObject));
                    else
                        result = obj;
                }
                catch (JsonException e)
                {
                    result = ErrorCode_Singer.JsonParseError.ToErrorBuilder(e);
                }

                yield return result;
            }
        }
    }
}

/// <summary>
/// Json converter for singer entities
/// </summary>
public class SingerJsonConverter : JsonConverter<SingerObject>
{
    private SingerJsonConverter() { }

    /// <summary>
    /// The instance
    /// </summary>
    public static SingerJsonConverter Instance { get; } = new();

    /// <inheritdoc />
    public override SingerObject Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options)
    {
        var readerClone = reader;

        if (readerClone.TokenType != JsonTokenType.StartObject)
            throw new JsonException($"Expected {JsonTokenType.StartObject}");

        readerClone.Read();

        if (readerClone.TokenType != JsonTokenType.PropertyName)
            throw new JsonException($"Expected {JsonTokenType.PropertyName}");

        var propertyName = readerClone.GetString();

        if (propertyName is null
         || !propertyName.Equals("Type", StringComparison.OrdinalIgnoreCase))
        {
            throw new JsonException("First Property should be 'Type'");
        }

        readerClone.Read();

        if (readerClone.TokenType != JsonTokenType.String)
        {
            throw new JsonException("'Type' should be a string");
        }

        var typeName = readerClone.GetString()!.ToLowerInvariant();

        SingerObject singerObject = typeName switch
        {
            "schema" => JsonSerializer.Deserialize<SingerSchema>(ref reader)!,
            "record" => JsonSerializer.Deserialize<SingerRecord>(ref reader)!,
            "state"  => JsonSerializer.Deserialize<SingerState>(ref reader)!,
            _ => throw new JsonException(
                $"Expected 'schema', 'record', or 'state'. Got '{typeName}'"
            )
        };

        return singerObject;
    }

    /// <inheritdoc />
    public override void Write(
        Utf8JsonWriter writer,
        SingerObject value,
        JsonSerializerOptions options)
    {
        throw new NotImplementedException();
    }
}

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable 8618
/// <summary>
/// A singer Record, Schema, or State
/// </summary>
public abstract class SingerObject
{
    [JsonPropertyName("type")] public string Type { get; set; }
}

/// <summary>
/// A singer Record
/// </summary>
public sealed class SingerRecord : SingerObject
{
    [JsonPropertyName("stream")] public string Stream { get; set; }

    [JsonPropertyName("record")] public JsonElement Record { get; set; }

    [JsonPropertyName("time_extracted")] public DateTime TimeExtracted { get; set; }
}

/// <summary>
/// A singer Schema
/// </summary>
public sealed class SingerSchema : SingerObject
{
    [JsonPropertyName("stream")] public string Stream { get; set; }

    [JsonPropertyName("key_properties")] public string[] KeyProperties { get; set; }

    [JsonPropertyName("bookmark_properties")]
    public string[] BookmarkProperties { get; set; }

    [JsonPropertyName("schema")] public JsonSchema Schema { get; set; }
}

public sealed class SingerState : SingerObject
{
    [JsonPropertyName("value")] public JsonElement Value { get; set; }
}
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
#pragma warning restore 8618
