import spark_project._

libraryDependencies ++= Seq(
  "com.klaytn.caver" % "core" % Versions.caver
)

excludeDependencies += "junit"           % "junit"
excludeDependencies += "org.slf4j"       % "slf4j-log4j12"
excludeDependencies += "log4j"           % "log4j"
excludeDependencies += "commons-logging" % "commons-logging"

javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint")
