package io.klaytn.tools.client

import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper
import com.amazonaws.regions.Regions
import com.amazonaws.services.elasticmapreduce.model._
import com.amazonaws.services.elasticmapreduce.{
  AmazonElasticMapReduce,
  AmazonElasticMapReduceClientBuilder
}
import io.klaytn.tools.model.emr.{EMRCluster, EMRState, EMRStep}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.JavaConverters._

// todo code refactoring
class EMRClient(optCredentialProfile: Option[String]) {
  @transient lazy val client: AmazonElasticMapReduce =
    AmazonElasticMapReduceClientBuilder.standard
      .withRegion(Regions.AP_NORTHEAST_2)
      .withCredentials(new EC2ContainerCredentialsProviderWrapper)
      .build()

  def recoveryStep(stepName: String,
                   ignoreRunningState: Boolean): (Boolean, String) = {
    try {
      _recoveryStep(stepName, ignoreRunningState)
    } catch {
      case e: Throwable =>
        val msg = s"""${getClass.getSimpleName}
                     |error: ${e.getMessage}
                     |stacktrace: ${StringUtils.abbreviate(
                       ExceptionUtils.getStackTrace(e),
                       800)}
                     |""".stripMargin
        (false, msg)
    }
  }

  private def _recoveryStep(stepName: String,
                            ignoreRunningState: Boolean): (Boolean, String) = {
    val clusterListRequest = new ListClustersRequest()
      .withClusterStates(
        Seq(EMRState.WAITING, EMRState.RUNNING)
          .map(_.toString)
          .asJavaCollection)

    val jobSteps = client
      .listClusters(clusterListRequest)
      .getClusters
      .asScala
      .flatMap { cluster =>
        client
          .listSteps(new ListStepsRequest()
            .withClusterId(cluster.getId))
          .getSteps
          .asScala
          .map(x => (x, cluster.getId))
      }
      .filter(x => x._1.getName == stepName)
      .filter(x =>
        x._1.getStatus.getTimeline.getStartDateTime != null || x._1.getStatus.getTimeline.getEndDateTime != null)

    if (jobSteps.isEmpty) {
      return (false, "history jobSteps is empty")
    }

    val sorted = jobSteps.sortBy {
      case (step, _) =>
        val timeline = step.getStatus.getTimeline
        val dateTime = Option(timeline)
          .map(_.getEndDateTime)
          .filter(_ != null)
          .getOrElse(timeline.getStartDateTime)
        dateTime.getTime
    }(Ordering[Long].reverse)

    if (sorted.isEmpty) {
      (false, "history not found")
    } else {
      val lastStepTuple = sorted.head
      recoveryStep(EMRStep.of(lastStepTuple._1, lastStepTuple._2),
                   ignoreRunningState)
    }
  }

  private def recoveryStep(originStepMeta: EMRStep,
                           ignoreRunningState: Boolean): (Boolean, String) = {
    val step = client
      .describeStep(
        new DescribeStepRequest()
          .withClusterId(originStepMeta.clusterId)
          .withStepId(originStepMeta.id))
      .getStep

    if (step == null) {
      return (false,
              s"cannot find step in ${originStepMeta.clusterId} cluster's stepId ${originStepMeta.id}")
    }

    if (!ignoreRunningState && EMRState.isRunningOrPending(step)) {
      return (false, "running step cannot be targeted")
    }

    val properties = step.getConfig.getProperties
      .entrySet()
      .asScala
      .filter(_ != null)
      .map(e => new KeyValue(e.getKey, e.getValue))
      .asJavaCollection

    val copiedStepConfig = new HadoopJarStepConfig()
      .withJar(step.getConfig.getJar)
      .withArgs(step.getConfig.getArgs)
      .withMainClass(step.getConfig.getMainClass)
      .withProperties(properties)

    val resubmitStepConfig = new StepConfig()
      .withName(originStepMeta.name)
      .withHadoopJarStep(copiedStepConfig)

    val result = client.addJobFlowSteps(
      new AddJobFlowStepsRequest()
        .withJobFlowId(originStepMeta.clusterId)
        .withSteps(resubmitStepConfig))

    val stepMessage = Option(result.getStepIds)
      .filter(!_.isEmpty)
      .map(jobs => jobs.get(0))
      .map(stepId => s"added stepId: $stepId")
      .getOrElse("")

    (!result.getStepIds.isEmpty, s"$stepMessage")
  }

  def getRunningClusters(): Seq[EMRCluster] = {
    val clusterListRequest = new ListClustersRequest()
      .withClusterStates(Seq(EMRState.RUNNING.toString).asJavaCollection)

    client.listClusters(clusterListRequest).getClusters.asScala.map { cluster =>
      val stepRequest = new ListStepsRequest()
        .withClusterId(cluster.getId)
        .withStepStates(
          Seq(EMRState.RUNNING, EMRState.PENDING)
            .map(_.toString)
            .asJavaCollection)

      val runningSteps = client
        .listSteps(stepRequest)
        .getSteps
        .asScala
        .map(summary => EMRStep.of(summary, cluster.getId))

      EMRCluster(
        id = cluster.getId,
        name = cluster.getName,
        state = EMRState.get(cluster.getStatus.getState),
        steps = runningSteps,
      )
    }
  }
}
