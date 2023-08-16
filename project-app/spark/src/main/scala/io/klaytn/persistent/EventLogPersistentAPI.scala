package io.klaytn.persistent

import io.klaytn.model.{ChainPhase, RefinedEventLog}
import io.klaytn.persistent.impl.rdb.RDBEventLogPersistentAPI

object EventLogPersistentAPI {
  def of(chainPhase: ChainPhase): EventLogPersistentAPI = {
    chainPhase.chain match {
      case _ =>
        new RDBEventLogPersistentAPI()
    }
  }
}

trait EventLogPersistentAPI extends Serializable {
  def insertEventLogs(eventLogs: Seq[RefinedEventLog]): Unit
  def getEventLogsByBlockRange(from: Long, to: Long): Seq[RefinedEventLog]
}
