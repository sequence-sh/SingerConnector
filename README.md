# Sequence® Singer Connector

[Reductech Sequence®](https://gitlab.com/reductech/sequence) is a collection of
libraries that automates cross-application e-discovery and forensic workflows.

This connector allows Sequence to act as a Singer target, converting data that
stream from Singer taps into entities.

## Steps

|     Step     | Description                                                           |   Result Type   |
| :----------: | :-------------------------------------------------------------------- | :-------------: |
| `FromSinger` | Convert data streaming from Singer into entities for use in Sequence. | `Array<Entity>` |

## Examples

To stream data from Slack into Sequence:

```scala
tap-slack --config C:\Singer\slack.config --catalog C:\Singer\catalog.json |
    .\sequence run scl "ReadStandardIn | FromSinger handlestate: (<>=>DoNothing) | Foreach (log <>['name'])"
```

# Releases

Can be downloaded from the [Releases page](https://gitlab.com/reductech/sequence/connectors/singer/-/releases).

# NuGet Packages

Are available in the [Reductech Nuget feed](https://gitlab.com/reductech/nuget/-/packages).
