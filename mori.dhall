let Schema =
      https://raw.githubusercontent.com/shinzui/mori-schema/026ae74331e5c516542af1dd96f041c658ed4621/package.dhall
        sha256:18258ef583580a897f4af3e7c86db0342afb42fb40efc535b217ba1089230141

in  Schema.Project::{
    , project = Schema.ProjectIdentity::{
      , name = "hw-kafka-streamly"
      , namespace = "shinzui"
      , type = Schema.PackageType.Library
      , description = Some
          "Streamly streaming bindings for hw-kafka-client — composable streams for consuming and folds for producing Kafka messages"
      , language = Schema.Language.Haskell
      , lifecycle = Schema.Lifecycle.Active
      , domains = [ "kafka", "streaming" ]
      , owners = [ "shinzui" ]
      }
    , repos =
      [ Schema.Repo::{
        , name = "hw-kafka-streamly"
        , github = Some "shinzui/hw-kafka-streamly"
        }
      ]
    , packages =
      [ Schema.Package::{
        , name = "hw-kafka-streamly"
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
      , Schema.Package::{
        , name = "hw-kafka-streamly-jitsurei"
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
    , dependencies = [ "haskell-works/hw-kafka-client", "composewell/streamly" ]
    }
