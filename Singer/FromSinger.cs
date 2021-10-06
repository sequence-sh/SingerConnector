using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Json.Schema;
using Reductech.EDR.Connectors.Singer.Errors;
using Reductech.EDR.Connectors.StructuredData.Errors;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Enums;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Reductech.EDR.Core.Steps;
using Reductech.EDR.Core.Util;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.EDR.Connectors.Singer
{

/// <summary>
/// Extracts the data from a Singer Tap and converts it to entities
/// </summary>
public sealed class FromSinger : CompoundStep<Array<Entity>>
{
    /// <inheritdoc />
    protected override async Task<Result<Array<Entity>, IError>> Run(
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
    [DefaultValueExplanation("Log the state")]
    public LambdaFunction<Entity, Unit> HandleState { get; set; } =
        new(null, new Log<Entity>() { Value = new GetAutomaticVariable<Entity>() });

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<FromSinger, Array<Entity>>();

    private static async IAsyncEnumerable<Entity> ReadSingerStreamEntities(
        StringStream stringStream,
        IStep step,
        IStateMonad stateMonad,
        LambdaFunction<Entity, Unit> handleState,
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
                            ErrorCodeStructuredData.SchemaViolation
                                .ToErrorBuilder(message)
                                .WithLocationSingle(step)
                        );
                }

                yield return CreateEntity(singerRecord.Record);
            }
            else if (result.Value is SingerState singerState)
            {
                var stateEntity = CreateEntity(singerState.Value);

                var scopedMonad = new ScopedStateMonad(
                    stateMonad,
                    currentState,
                    handleState.VariableNameOrItem,
                    new KeyValuePair<VariableName, object>(
                        handleState.VariableNameOrItem,
                        stateEntity
                    )
                );

                var handleStateResult =
                    await handleState.StepTyped.Run(scopedMonad, cancellationToken);

                if (handleStateResult.IsFailure)
                    throw new ErrorException(handleStateResult.Error);
            }
        }
    }

    private static Entity CreateEntity(JsonElement element)
    {
        var ev = CreateEntityValue(element);

        if (ev is EntityValue.NestedEntity nestedEntity)
            return nestedEntity.Value;

        return new Entity(
            ImmutableDictionary<string, EntityProperty>.Empty
                .Add(Entity.PrimitiveKey, new EntityProperty(Entity.PrimitiveKey, ev, null, 0))
        );
    }

    private static EntityValue CreateEntityValue(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Undefined: return EntityValue.Null.Instance;
            case JsonValueKind.Object:
            {
                var dict = element.EnumerateObject()
                    .Select(
                        (x, i) =>
                            new EntityProperty(x.Name, CreateEntityValue(x.Value), null, i)
                    )
                    .ToImmutableDictionary(x => x.Name);

                var entity = new Entity(dict);
                return new EntityValue.NestedEntity(entity);
            }
            case JsonValueKind.Array:
            {
                var list = element.EnumerateArray().Select(CreateEntityValue).ToImmutableList();
                return new EntityValue.NestedList(list);
            }
            case JsonValueKind.String: return new EntityValue.String(element.GetString()!);
            case JsonValueKind.Number:
            {
                if (element.TryGetInt32(out var i))
                    return new EntityValue.Integer(i);

                return new EntityValue.Double(element.GetDouble());
            }
            case JsonValueKind.True: return new EntityValue.Boolean(true);
            case JsonValueKind.False: return new EntityValue.Boolean(false);
            case JsonValueKind.Null: return EntityValue.Null.Instance;
            default: throw new ArgumentOutOfRangeException(element.ValueKind.ToString());
        }
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
                var obj = JsonSerializer.Deserialize<SingerObject>(line, options);

                if (obj is null)
                {
                    yield return ErrorCode.CouldNotParse.ToErrorBuilder(line, nameof(SingerObject));
                }
                else
                {
                    yield return obj;
                }
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
        Utf8JsonReader readerClone = reader;

        if (readerClone.TokenType != JsonTokenType.StartObject)
            throw new JsonException();

        readerClone.Read();

        if (readerClone.TokenType != JsonTokenType.PropertyName)
            throw new JsonException();

        var propertyName = readerClone.GetString();

        if (propertyName is null
         || !propertyName.Equals("Type", StringComparison.OrdinalIgnoreCase))
        {
            throw new JsonException();
        }

        readerClone.Read();

        if (readerClone.TokenType != JsonTokenType.String)
        {
            throw new JsonException();
        }

        string typeName = readerClone.GetString()!.ToLowerInvariant();

        SingerObject singerObject = typeName switch
        {
            "schema" => JsonSerializer.Deserialize<SingerSchema>(ref reader)!,
            "record" => JsonSerializer.Deserialize<SingerRecord>(ref reader)!,
            "state"  => JsonSerializer.Deserialize<SingerState>(ref reader)!,
            _        => throw new JsonException()
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

}
