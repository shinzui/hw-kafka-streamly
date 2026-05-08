let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/1f70781427426c09673d46f8e6733b7e7d0abedc/package.dhall
        sha256:3b79aae9216456678300441ca8616b64a4b4fa520a1286dfcc418f60899d5d4a

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
    }
