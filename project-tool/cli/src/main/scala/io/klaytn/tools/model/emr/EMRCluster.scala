package io.klaytn.tools.model.emr

case class EMRCluster(id: String,
                      name: String,
                      state: EMRState.Value,
                      steps: Seq[EMRStep])
