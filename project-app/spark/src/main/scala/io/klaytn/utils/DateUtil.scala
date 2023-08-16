package io.klaytn.utils

import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone, LocalDate, Period}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

object DateUtil {
  // == DateTimeZone.forID("Asia/Seoul")
  val KST: DateTimeZone = DateTimeZone.forOffsetHours(9)

  def toUTCString(timestamp: Timestamp): String = {
    toUTCString(datetime(timestamp))
  }

  def toUTCString(timestamp: Timestamp, pattern: String): String = {
    toUTCString(datetime(timestamp), pattern)
  }

  def toUTCString(dateTime: DateTime): String = {
    dateTime.toDateTime(DateTimeZone.UTC).toString("yyyy-MM-dd HH:mm:ss")
  }

  def toUTCString(dateTime: DateTime, pattern: String): String = {
    dateTime.toDateTime(DateTimeZone.UTC).toString(pattern)
  }

  def toUTCISOString(dateTime: DateTime): String = {
    dateTime
      .toDateTime(DateTimeZone.UTC)
      .toDateTimeISO
      .toString(ISODateTimeFormat.dateTime().withOffsetParsed())
  }

  def toKSTISOString(dateTime: DateTime): String = {
    dateTime
      .toDateTime(KST)
      .toDateTimeISO
      .toString(ISODateTimeFormat.dateTime().withOffsetParsed())
  }

  /**
    * Apply the Korean time zone Timezone(Asia/Seoul).
    * Example: Returns a UTC-based value read from a storage (DB) with the KST time zone applied.
    */
  def toKST(dateTime: DateTime): DateTime = {
    dateTime.toDateTime(KST)
  }

  def toKST(timestamp: Timestamp): DateTime = {
    toKST(DateUtil.datetime(timestamp))
  }

  /**
    * Applies the time zone (UTC).
    *
    * 예)
    * <Input>
    * 2019-07-29T09:00:00.000+09:00 (KST)
    *
    * <Output>
    * 2019-07-29T00:00:00.000Z
    */
  def toUTC(dateTime: DateTime): DateTime = {
    dateTime.toDateTime(DateTimeZone.UTC)
  }

  def today(): DateTime = new DateTime()

  def todayUTC(): DateTime = toUTC(today())

  def todayTs(): Timestamp = {
    new Timestamp(today().getMillis)
  }

  def todayTsAtStartOfDay(): Long = {
    todayUTC().withTimeAtStartOfDay().getMillis
  }

  def yesterdayTsAtEndOfDay(): Long = {
    dayAgoTsAtEndOfDay(1)
  }

  def dayAgoTsAtStartOfDay(day: Int): Long = {
    todayTsAtStartOfDay() - (86400000 * day)
  }

  def dayAgoTsAtEndOfDay(day: Int): Long = {
    todayTsAtStartOfDay() - (86400000 * (day - 1)) - 1
  }

  /**
    * Cuts from time to milliseconds or less.
    *
    * 예)
    * <Input>
    * 2019-08-02T11:37:26.687+09:00
    *
    * <Output>
    * 2019-08-02T11:37:26.000+09:00
    *
    */
  def roundMillis(dateTime: DateTime): DateTime = {
    dateTime.withMillisOfSecond(0)
  }

  /**
    * Cuts from time to seconds or less.
    *
    * 예)
    * <Input>
    * 2019-08-02T11:37:26.687+09:00
    *
    * <Output>
    * 2019-08-02T11:37:00.000+09:00
    *
    */
  def roundSecond(dateTime: DateTime): DateTime = {
    dateTime.withSecondOfMinute(0).withMillisOfSecond(0)
  }

  def roundSecond(ts: Timestamp): DateTime = {
    datetime(ts).withSecondOfMinute(0).withMillisOfSecond(0)
  }

  /**
    * Cuts from hours to minutes or less.
    * <Input>
    * 2020-08-02T11:37:26.687+09:00
    *
    * <Output>
    * 2020-08-02T11:300:00.000+09:00
    */
  def roundMinute(dateTime: DateTime): DateTime = {
    dateTime.withMillisOfSecond(0).withSecondOfMinute(0).withMinuteOfHour(0)
  }

  /**
    * Returns the greater of the two time values.
    */
  def max(a: Timestamp, b: Timestamp): Timestamp = {
    if (a.after(b)) a else b
  }

  /**
    * Returns the smaller of the two time values.
    */
  def min(a: Timestamp, b: Timestamp): Timestamp = {
    if (a.before(b)) a else b
  }

  def parse(dateTimeStr: String, pattern: String): DateTime = {
    parse(dateTimeStr, pattern, DateTimeZone.UTC)
  }

  def parse(dateTimeStr: String): DateTime = {
    parse(dateTimeStr, DateTimeZone.UTC)
  }

  def parse(dateTimeStr: String, timezone: DateTimeZone): DateTime = {
    if (dateTimeStr.contains("T")) {
      return parseISO(dateTimeStr)
    }
    val normalized = dateTimeStr.replaceAll("\\.|\\s|:|-|/|_", "")
    val dateTime = normalized.length match {
      case 4  => DateUtil.parse(normalized, "yyyy", timezone)
      case 6  => DateUtil.parse(normalized, "yyyyMM", timezone)
      case 8  => DateUtil.parse(normalized, "yyyyMMdd", timezone)
      case 10 => DateUtil.parse(normalized, "yyyyMMddHH", timezone)
      case 12 => DateUtil.parse(normalized, "yyyyMMddHHmm", timezone)
      case 14 => DateUtil.parse(normalized, "yyyyMMddHHmmss", timezone)
      case 15 =>
        DateUtil.parse(normalized.substring(0, 14), "yyyyMMddHHmmss", timezone)
      case 16 =>
        DateUtil.parse(normalized.substring(0, 14), "yyyyMMddHHmmss", timezone)
      case 17 | 20 => // It does not support nano seconds. Cutting to seconds
        DateUtil.parse(normalized.substring(0, 14), "yyyyMMddHHmmss", timezone)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported time format => $normalized")
    }
    dateTime
  }

  /**
    * ex)
    * DateUtil.parse("2019-09-16 00:00:00", "yyyy-MM-dd HH:mm:ss")
    */
  def parse(dateTimeStr: String,
            pattern: String,
            timezone: DateTimeZone): DateTime = {
    val fmt = DateTimeFormat.forPattern(pattern).withZone(timezone)
    DateTime.parse(dateTimeStr, fmt)
  }

  /**
    * ISO 8601: https://en.wikipedia.org/wiki/ISO_8601
    * Always use offset.
    * ex) 2020-07-02T19:17:51+00:00
    */
  def parseISO(dateTimeStr: String): DateTime = {
    new DateTime(
      ISODateTimeFormat.dateOptionalTimeParser
        .withOffsetParsed()
        .parseDateTime(dateTimeStr))
  }

  def parseKST(dateTimeStr: String): DateTime = {
    parse(dateTimeStr, KST)
  }

  def timestampNow(): Timestamp = {
    new Timestamp(DateTime.now().getMillis)
  }

  def timestamp(dateTimeStr: String): Timestamp = {
    val dt = parse(dateTimeStr)
    new Timestamp(dt.getMillis)
  }

  def timestamp(dateTime: DateTime): Timestamp = {
    new Timestamp(dateTime.getMillis)
  }

  def timestampKST(dateTimeStr: String): Timestamp = {
    val dt = parse(dateTimeStr, KST)
    new Timestamp(dt.getMillis)
  }

  def toString(ts: Timestamp, pattern: String): String = {
    val df: SimpleDateFormat = new SimpleDateFormat(pattern)
    df.format(ts)
  }

  def toString(dateTime: DateTime, pattern: String): String = {
    toString(timestamp(dateTime), pattern)
  }

  def toStringKST(dateTime: DateTime, pattern: String): String = {
    toKST(dateTime).toString(pattern)
  }

  def toStringKST(ts: Timestamp, pattern: String): String = {
    toStringKST(datetime(ts), pattern)
  }

  def datetime(timestamp: Timestamp): DateTime = {
    new DateTime(timestamp.getTime)
  }

  def firstDayOfMonth(t: DateTime): DateTime = {
    t.dayOfMonth().withMinimumValue().withTimeAtStartOfDay()
  }

  def firstDayOfMonth(): DateTime = {
    nowKST().dayOfMonth().withMinimumValue().withTimeAtStartOfDay()
  }

  def nowKST(): DateTime = {
    DateTime.now(KST)
  }

  def nowDayKST(): DateTime = {
    DateUtil.parseKST(DateUtil.nowKST().toString("yyyy.MM.dd"))
  }

  def nowHourKST(): DateTime = {
    DateUtil.parseKST(DateUtil.nowKST().toString("yyyy.MM.dd HH"))
  }

  def timestampNowKST(): Timestamp = {
    timestamp(DateTime.now(KST))
  }

  def toFromToTimeStamp(occuredDate: String): (Timestamp, Timestamp) = {
    val from: Timestamp = DateUtil.timestamp(DateUtil.parseKST(occuredDate))
    val to: Timestamp =
      DateUtil.timestamp(DateUtil.parseKST(occuredDate).plusDays(1))
    (from, to)
  }

  def toFromToQuarterSalesAgency(dayStr: String) = {
    val year = DateUtil.parse(dayStr).getYear
    val month = DateUtil.parse(dayStr).getMonthOfYear
    val baseQuarter = (month - 1) / 3
    if (baseQuarter == 0) {
      val lastYear = year - 1
      val quarter = s"${lastYear}_4"
      (s"${lastYear}10", s"${lastYear}12", s"${quarter}")
    } else {
      val toMonth = baseQuarter * 3
      val fromMonth = toMonth - 2
      val quarter = s"${year}_${baseQuarter}"
      (f"$year$fromMonth%02d", f"$year$toMonth%02d", quarter)
    }
  }

  def split(stepMillis: Int,
            from: DateTime,
            to: DateTime): Seq[(DateTime, DateTime)] = {
    if (from.isAfter(to)) {
      throw new IllegalArgumentException(
        s"Wrong Parameter. from = $from, to = $to")
    }
    val ranges = ListBuffer[(DateTime, DateTime)]()
    var start = from
    var end = from.plusMillis(stepMillis)
    while (end.isBefore(to)) {
      ranges.append((start, end))
      start = end
      end = start.plusMillis(stepMillis)
    }
    if (end.isEqual(to) || end.isAfter(to)) {
      ranges.append((start, to))
    }
    ranges
  }

  def range(stepMillis: Int, from: DateTime, to: DateTime): Seq[DateTime] = {
    if (from.isAfter(to)) {
      throw new IllegalArgumentException(
        s"Wrong Parameter. from = $from, to = $to")
    }
    Stream
      .iterate(from)(_.plusMillis(stepMillis))
      .takeWhile(_.getMillis <= to.getMillis)
  }

  def rangeExclusive(stepMillis: Int,
                     from: DateTime,
                     until: DateTime): Seq[DateTime] = {
    if (from.isAfter(until)) {
      throw new IllegalArgumentException(
        s"Wrong Parameter. from = $from, until = $until")
    }
    Stream
      .iterate(from)(_.plusMillis(stepMillis))
      .takeWhile(_.getMillis < until.getMillis)
  }

  /**
    * Get the last day of the month. Exception handling for the last day of the month of February due to leap year
    * ex)
    * 202102xx ==> 29
    * 202102xx ==> 28
    * 203602xx ==> 28
    */
  def lastDayOfMonth(dt: DateTime): Int = {
    dt.dayOfMonth().withMaximumValue().getDayOfMonth
  }

  def getMinTimestamp(timestamps: Seq[Timestamp]): Option[Timestamp] = {
    implicit def ordered = new Ordering[Timestamp] {
      def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
    }

    if (timestamps.nonEmpty) {
      Some(timestamps.min)
    } else {
      None
    }
  }

  def dayIterator(from: LocalDate, to: LocalDate): Iterator[LocalDate] =
    Iterator
      .iterate(from)(_.plus(new Period().withDays(1)))
      .takeWhile(!_.isAfter(to))

  def dayIterator(from: String, to: String): Iterator[LocalDate] =
    dayIterator(new LocalDate(from), new LocalDate(to))

  def yearIterator(from: String, to: String): Iterator[LocalDate] =
    Iterator
      .iterate(LocalDate.parse(from))(_.plus(new Period().withYears(1)))
      .takeWhile(!_.isAfter(LocalDate.parse(to)))

}
