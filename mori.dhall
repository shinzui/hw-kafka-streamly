let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/ad9960dd3dd3b33eadd45f17bcf430b0e1ec13bc/package.dhall
        sha256:83aa1432e98db5da81afde4ab2057dcab7ce4b2e883d0bc7f16c7d25b917dd0c

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
