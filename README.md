# Sequence Singer Connector

[Sequence®](https://sequence.sh) is a collection of libraries for
automation of cross-application e-discovery and forensic workflows.

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

# Documentation

https://sequence.sh

# Download

https://sequence.sh/download

# Try SCL and Core

https://sequence.sh/playground

# Package Releases

Can be downloaded from the [Releases page](https://gitlab.com/sequence/connectors/singer/-/releases).

# NuGet Packages

Release nuget packages are available from [nuget.org](https://www.nuget.org/profiles/Sequence).
