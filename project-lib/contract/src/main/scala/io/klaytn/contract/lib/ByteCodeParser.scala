package io.klaytn.contract.lib

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ByteCodeParser {
  def extractFunctionSignatures(byteCodes: String): Set[String] = {
    if (byteCodes.isEmpty) {
      return Set.empty[String]
    }

    val functionSignatures = mutable.Set.empty[String]

    val bytes = byteCodes.substring(2).trim().grouped(2)

    var push = false
    var push4 = false
    var pushCount = 0
    val opValues = new StringBuilder()

    bytes.foreach { opCode =>
      if (push) {
        if (pushCount > 0 && push4) {
          pushCount -= 1
          opValues.append(opCode)
        } else {
          if (opValues.nonEmpty) {
            functionSignatures.add(s"0x${opValues.toString}")
            opValues.clear()
          }

          pushCount = 0
          push = false
          push4 = false
        }
      }

      if (!push) {
        if (opCode >= "60" && opCode <= "7f") {
          push = true
          push4 = opCode == "63"
          if (Constants.OPCodes.contains(opCode)) {
            pushCount = Constants.OPCodes(opCode).replaceFirst("PUSH", "").toInt
          }
        }
      }
    }

    functionSignatures.toSet
  }

  def byteCodesToOpCodes(byteCodes: String): Seq[String] = {
    if (byteCodes.isEmpty) {
      return Seq.empty[String]
    }

    val codes = ArrayBuffer.empty[String]

    val bytes = byteCodes.substring(2).trim().grouped(2)

    var push = false
    var pushCount = 0

    val previousOpCode = new StringBuilder()

    bytes.foreach { opCode =>
      if (push) {
        if (pushCount > 0) {
          pushCount -= 1
          previousOpCode.append(opCode)
        } else {
          codes.append(previousOpCode.toString())
          previousOpCode.clear()
          pushCount = 0
          push = false
        }
      }

      if (!push) {
        if (opCode >= "60" && opCode <= "7f") {
          push = true
          previousOpCode.append(Constants.OPCodes(opCode)).append(" ")
          pushCount = Constants.OPCodes(opCode).replaceFirst("PUSH", "").toInt
        } else {
          codes.append(Constants.OPCodes.getOrElse(opCode, s"unknown($opCode)"))
        }
      }
    }

    codes
  }
}
