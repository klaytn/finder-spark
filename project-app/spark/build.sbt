import spark_project._

libraryDependencies ++= Dependencies.sparkProvided

libraryDependencies ++= Seq(
  "com.typesafe"                  % "config"                      % Versions.typeSafeConfig,
  "org.apache.spark"              %% "spark-streaming-kafka-0-10" % Versions.spark,
  "com.amazonaws"                 % "aws-java-sdk-s3"             % Versions.awsSDK,
  "mysql"                         % "mysql-connector-java"        % Versions.mysql,
  "com.zaxxer"                    % "HikariCP"                    % Versions.hikari,
  "com.github.ben-manes.caffeine" % "caffeine"                    % Versions.caffeine,
  "com.typesafe.scala-logging"    %% "scala-logging"              % "3.9.4",
  "ch.qos.logback"                % "logback-classic"             % "1.2.10",
  "org.reactivestreams"           % "reactive-streams"            % "1.0.3",
  "org.redisson"                  % "redisson"                    % Versions.redisson,
  "com.hubspot.jinjava"           % "jinjava"                     % Versions.jinjava,
  "com.lihaoyi"                   %% "requests"                   % Versions.requests,
  "com.amazonaws"                 % "aws-java-sdk-sqs"            % Versions.awsSDK,
  "org.apache.httpcomponents"     % "httpclient"                  % "4.5.14",
  "com.google.cloud"              % "google-cloud-storage"        % Versions.gcs,
)

libraryDependencies ++= Dependencies.circe
libraryDependencies ++= Dependencies.json4s
libraryDependencies ++= Dependencies.jackson
dependencyOverrides ++= Dependencies.jackson

excludeDependencies += "org.slf4j" % "slf4j-log4j12"
//excludeDependencies += "commons-logging" % "commons-logging"

javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint")
