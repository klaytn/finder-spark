package io.klaytn.persistent.impl.rdb

import io.klaytn.model.RefinedEventLog
import io.klaytn.persistent.EventLogPersistentAPI
import io.klaytn.repository.EventLogRepository

class RDBEventLogPersistentAPI
    extends EventLogRepository
    with EventLogPersistentAPI {
  override def insertEventLogs(eventLogs: Seq[RefinedEventLog]): Unit = {
    super.insertEventLogs(eventLogs)
  }

  override def getEventLogsByBlockRange(from: Long,
                                        to: Long): Seq[RefinedEventLog] = {
    super.getEventLogsByBlockRange(from, to)
  }
}
