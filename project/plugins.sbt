logLevel := Level.Warn

resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.1")
//addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.7")
addSbtPlugin("com.eed3si9n"     % "sbt-assembly"         % "0.15.0")
addSbtPlugin("com.geirsson"     % "sbt-scalafmt"         % "1.5.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")
