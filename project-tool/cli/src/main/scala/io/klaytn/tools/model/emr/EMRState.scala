package io.klaytn.tools.model.emr

import com.amazonaws.services.elasticmapreduce.model.Step

//noinspection TypeAnnotation
object EMRState extends Enumeration {
  val COMPLETED = Value("COMPLETED")
  val RUNNING = Value("RUNNING")
  val WAITING = Value("WAITING")
  val PENDING = Value("PENDING")
  val FAILED = Value("FAILED")

  private val byUppercaseValue =
    values.map(x => (x.toString.toUpperCase(), x)).toMap

  def get(value: String): EMRState.Value = {
    val optState = Option(value)
      .filter(_.nonEmpty)
      .map(_.toUpperCase)
      .flatMap(upperCaseValue => byUppercaseValue.get(upperCaseValue))
    if (optState.isEmpty) {
      throw new IllegalArgumentException(s"invalid string value: $value")
    }
    optState.get
  }
  def isRunningOrPending(step: Step): Boolean = {
    Seq(RUNNING, PENDING).map(_.toString).contains(step.getStatus.getState)
  }

}
