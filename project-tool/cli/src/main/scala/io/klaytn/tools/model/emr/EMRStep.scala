package io.klaytn.tools.model.emr

import com.amazonaws.services.elasticmapreduce.model.StepSummary

case class EMRStep(id: String,
                   name: String,
                   state: EMRState.Value,
                   clusterId: String)
object EMRStep {
  def of(stepSummary: StepSummary, clusterId: String): EMRStep = {
    EMRStep(
      id = stepSummary.getId,
      name = stepSummary.getName,
      state = EMRState.get(stepSummary.getStatus.getState),
      clusterId = clusterId
    )
  }
}
