import spark_project._

libraryDependencies += "com.typesafe"       % "config"               % Versions.typeSafeConfig
libraryDependencies += "com.amazonaws"      % "aws-java-sdk-emr"     % Versions.awsSDK
libraryDependencies += "com.amazonaws"      % "aws-java-sdk-s3"      % Versions.awsSDK
libraryDependencies += "com.lihaoyi"        %% "requests"            % "0.7.0"
libraryDependencies += "org.apache.commons" % "commons-lang3"        % "3.12.0"
libraryDependencies += "com.zaxxer"         % "HikariCP"             % Versions.hikari
libraryDependencies += "mysql"              % "mysql-connector-java" % Versions.mysql

javacOptions ++= Seq("-source", "11", "-target", "11", "-Xlint")
