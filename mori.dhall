let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/8415b4b8a746a84eecf982f0f1d7194368bf7b54/package.dhall
        sha256:d19ae156d6c357d982a1aea0f1b6ba1f01d76d2d848545b150db75ed4c39a8a9

in  { project =
      { name = "hw-kafka-streamly"
      , namespace = "shinzui"
      , type = Schema.PackageType.Library
      , description = Some
          "Streamly streaming bindings for hw-kafka-client — composable stream sources for consuming and fold-based sinks for producing Kafka messages"
      , language = Schema.Language.Haskell
      , lifecycle = Schema.Lifecycle.Active
      , domains = [ "kafka", "streaming" ]
      , owners = [ "shinzui" ]
      , origin = Schema.Origin.Own
      }
    , repos =
      [ { name = "hw-kafka-streamly"
        , github = Some "shinzui/hw-kafka-streamly"
        , gitlab = None Text
        , git = None Text
        , localPath = None Text
        }
      ]
    , packages =
      [ { name = "hw-kafka-streamly"
        , type = Schema.PackageType.Library
        , language = Schema.Language.Haskell
        , path = Some "hw-kafka-streamly"
        , description = Some
            "Streamly streaming integration for hw-kafka-client"
        , lifecycle = None Schema.Lifecycle
        , visibility = Schema.Visibility.Public
        , runtime = { deployable = False, exposesApi = False }
        , runtimeEnvironment = None Schema.RuntimeEnvironment
        , dependencies =
          [ Schema.Dependency.ByName "hw-kafka-client"
          , Schema.Dependency.ByName "streamly-core"
          ]
        , docs = [] : List Schema.DocRef
        , config = [] : List Schema.ConfigItem
        , apiSource = None Schema.ApiSource
        }
      , { name = "hw-kafka-streamly-jitsurei"
        , type = Schema.PackageType.Application
        , language = Schema.Language.Haskell
        , path = Some "hw-kafka-streamly-jitsurei"
        , description = Some
            "Cookbook examples demonstrating hw-kafka-streamly usage patterns"
        , lifecycle = None Schema.Lifecycle
        , visibility = Schema.Visibility.Public
        , runtime = { deployable = False, exposesApi = False }
        , runtimeEnvironment = None Schema.RuntimeEnvironment
        , dependencies =
          [ Schema.Dependency.ByName "hw-kafka-streamly"
          , Schema.Dependency.ByName "hw-kafka-client"
          , Schema.Dependency.ByName "streamly-core"
          , Schema.Dependency.ByName "streamly"
          ]
        , docs = [] : List Schema.DocRef
        , config = [] : List Schema.ConfigItem
        , apiSource = None Schema.ApiSource
        }
      ]
    , bundles = [] : List Schema.PackageBundle
    , dependencies =
      [ "haskell-works/hw-kafka-client", "composewell/streamly" ]
    , apis = [] : List Schema.Api
    , agents = [] : List Schema.AgentHint
    , skills = [] : List Schema.Skill
    , subagents = [] : List Schema.Subagent
    , standards = [] : List Text
    , docs =
      [ { key = "exec-plan-streamly-bindings"
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
