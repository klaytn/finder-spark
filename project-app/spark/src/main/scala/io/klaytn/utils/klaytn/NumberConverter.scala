package io.klaytn.utils.klaytn

object NumberConverter {
  implicit class StringConverter(s: String) {
    def hexToBigInt(): BigInt = {
      if (s.isEmpty) {
        BigInt(0)
      } else if (s.startsWith("0x")) {
        val normalize0 = normalize(false)
        if (normalize0.isEmpty) {
          BigInt(0)
        } else {
          BigInt(normalize0, 16)
        }
      } else {
        BigInt(s, 16)
      }
    }

    def hexToInt(): Int = {
      hexToBigInt().toInt
    }

    def hexToLong(): Long = {
      hexToBigInt().toLong
    }

    def hexToBigDecimal(): BigDecimal = {
      BigDecimal(hexToBigInt())
    }

    def hexOrBigDecimalToBigInt(scale: Int): BigInt = {
      if (s.isEmpty) {
        BigInt(0)
      } else if (s.startsWith("0x")) {
        hexToBigInt()
      } else {
        BigDecimal(s).removeDecimalPoint(scale)
      }
    }

    def normalize(withPrefix0x: Boolean): String = {
      val result0 = s.replaceFirst("0x", "").replaceFirst("[0]*", "")
      val result = if (result0.isEmpty) "0" else result0
      if (withPrefix0x) s"0x$result"
      else result
    }

    def normalizeToAddress(): String = {
      val d1 = normalize(false)
      val d2 = "0" * (40 - d1.length)
      s"0x$d2$d1"
    }
  }

  implicit class BigDecimalConverter(bigDecimal: BigDecimal) {
    def to64BitsHex(scale: Int): String = {
      val bigInt = removeDecimalPoint(scale)
      f"0x${bigInt.toString(16)}%64s".replace(' ', '0')
    }

    def removeDecimalPoint(scale: Int): BigInt = {
      (bigDecimal * math.pow(10, scale)).toBigInt()
    }
  }

  implicit class BigIntConverter(bigInt: BigInt) {
    def to64BitsHex(): String = {
      f"0x${bigInt.toString(16)}%64s".replace(' ', '0')
    }

    def to128BitsHex(): String = {
      f"0x${bigInt.toString(16)}%128s".replace(' ', '0')
    }

    def toHex: String = {
      f"0x${bigInt.toString(16)}"
    }
  }
}
