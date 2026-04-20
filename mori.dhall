let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/9b1d6eea8027ae57576cf0712c0b9167fccbc1a9/package.dhall
        sha256:a19f5dd9181db28ba7a6a1b77b5ab8715e81aba3e2a8f296f40973003a0b4412

in  Schema.Project::{ project =
      Schema.ProjectIdentity::{ name = "hw-kafka-streamly"
      , namespace = "shinzui"
      , type = Schema.PackageType.Library
      , description = Some
          "Streamly streaming bindings for hw-kafka-client — composable stream sources for consuming and fold-based sinks for producing Kafka messages"
      , language = Schema.Language.Haskell
      , lifecycle = Schema.Lifecycle.Active
      , domains = [ "kafka", "streaming" ]
      , owners = [ "shinzui" ]
      }
    , repos =
      [ Schema.Repo::{ name = "hw-kafka-streamly"
        , github = Some "shinzui/hw-kafka-streamly"
        }
      ]
    , packages =
      [ Schema.Package::{ name = "hw-kafka-streamly"
        , type = Schema.PackageType.Library
        , language = Schema.Language.Haskell
        , path = Some "hw-kafka-streamly"
        , description = Some
            "Streamly streaming integration for hw-kafka-client"
        , dependencies =
          [ Schema.Dependency.ByName "hw-kafka-client"
          , Schema.Dependency.ByName "streamly-core"
          ]
        }
      , Schema.Package::{ name = "hw-kafka-streamly-jitsurei"
        , type = Schema.PackageType.Application
        , language = Schema.Language.Haskell
        , path = Some "hw-kafka-streamly-jitsurei"
        , description = Some
            "Cookbook examples demonstrating hw-kafka-streamly usage patterns"
        , dependencies =
          [ Schema.Dependency.ByName "hw-kafka-streamly"
          , Schema.Dependency.ByName "hw-kafka-client"
          , Schema.Dependency.ByName "streamly-core"
          , Schema.Dependency.ByName "streamly"
          ]
        }
      ]
    , dependencies =
      [ "haskell-works/hw-kafka-client", "composewell/streamly" ]
    , docs =
      [ Schema.DocRef::{ key = "exec-plan-streamly-bindings"
        , kind = Schema.DocKind.Spec
        , audience = Schema.DocAudience.Module
        , description = Some
            "Execution plan for building streamly bindings for hw-kafka-client"
        , location =
            Schema.DocLocation.LocalFile
              "docs/plans/1-streamly-bindings-for-hw-kafka-client.md"
        }
      ]
    }
