import spark_project._

name := "klaytn-spark"
version := "0.1"

/** ------------------------------------------------------
  *  Sub Project Definition
  * ------------------------------------------------------ */
lazy val contract =
  Projects
    .define("contract-lib", "project-lib/contract", includeDeps = true, includeScala = true)

lazy val datasource =
  Projects
    .define("datasource-lib", "project-lib/datasource", includeDeps = true, includeScala = true)

lazy val spark =
  Projects
    .define("klaytn-spark", "project-app/spark", includeDeps = true, includeScala = true)
    .dependsOn(contract)

lazy val cli =
  Projects
    .define("klaytn-cli", "project-tool/cli", includeDeps = true, includeScala = true)
    .dependsOn(datasource)
    .dependsOn(contract)
