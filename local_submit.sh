PROJECT_ROOT=$(pwd)
JAVA_CLASSPATH="${PROJECT_ROOT}/project-tool/cli/target/scala-2.12/klaytn-cli.jar"
JAVA_CLASSPATH="$JAVA_CLASSPATH:${PROJECT_ROOT}/project-app/spark/src/main/scala"
JAVA_CLASSPATH="$JAVA_CLASSPATH:${PROJECT_ROOT}/project-app/spark/src/main/resources"

PS3="Select chain: "
select chain in "cypress" "baobab"
do
    case $chain in
        "cypress")
            break
            ;;
        "baobab")
            break
            ;;
        *)
            echo "Invalid chain"
            exit 1
            break
            ;;
    esac
done

# If there is --build option
if [ "$1" = "--build" ]; then
  echo "Build jar file"
  sudo ./sbt klaytn-spark/clean klaytn-spark/assembly
fi


config_resource=$PROJECT_ROOT/project-app/spark/src/main/resources/prod/all.$chain.conf

echo "Config Resource: $config_resource"

echo "Executing spark-submit command:"

sudo spark-submit \
--master local[*] \
--conf spark.driver.memory=2g \
--conf spark.executor.memory=3g \
--conf spark.app.phase=prod \
--conf spark.app.chain=$chain \
--conf spark.yarn.maxAppAttempts=1 \
--conf "spark.driver.extraJavaOptions=-Dconfig.file=$config_resource" \
--conf "spark.executor.extraJavaOptions=-Dconfig.file=$config_resource" \
--conf spark.driver.extraClassPath=$JAVA_CLASSPATH \
--conf spark.executor.extraClassPath=$JAVA_CLASSPATH \
--name local-test \
--class io.klaytn.test.RegisterSearchTemplate \
project-app/spark/target/scala-2.12/klaytn-spark.jar
