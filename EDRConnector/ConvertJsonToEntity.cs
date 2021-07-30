using System;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using CSharpFunctionalExtensions;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Reductech.EDR.Core;
using Reductech.EDR.Core.Attributes;
using Reductech.EDR.Core.Entities;
using Reductech.EDR.Core.Internal;
using Reductech.EDR.Core.Internal.Errors;
using Entity = Reductech.EDR.Core.Entity;

namespace Reductech.Templates.EDRConnector
{

/// <summary>
/// Convert an entity from a JSON stream. Clone of StructuredData.FromJSON.
/// </summary>
public sealed class ConvertJsonToEntity : CompoundStep<Entity>
{
    /// <inheritdoc />
    protected override async Task<Result<Entity, IError>> Run(
        IStateMonad stateMonad,
        CancellationToken cancellationToken)
    {
        var text = await Stream.Run(stateMonad, cancellationToken).Map(x => x.GetStringAsync());

        if (text.IsFailure)
            return text.ConvertFailure<Entity>();

        Entity? entity;

        try
        {
            entity = JsonConvert.DeserializeObject<Entity>(
                text.Value,
                EntityJsonConverter.Instance
            );
        }
        catch (Exception e)
        {
            stateMonad.Log(LogLevel.Error, e.Message, this);
            entity = null;
        }

        if (entity is null)
            return Result.Failure<Entity, IError>(
                ErrorCode.CouldNotParse.ToErrorBuilder(text.Value, "JSON").WithLocation(this)
            );

        return entity;
    }

    /// <summary>
    /// Stream containing the Json data.
    /// </summary>
    [StepProperty(1)]
    [Required]
    public IStep<StringStream> Stream { get; set; } = null!;

    /// <inheritdoc />
    public override IStepFactory StepFactory { get; } =
        new SimpleStepFactory<ConvertJsonToEntity, Entity>();
}

}
