package io.klaytn.model

import io.klaytn.model.EventLogType.{
  NTFTransfer,
  TokenTransfer,
  TransferBatch,
  TransferSingle
}
import io.klaytn.utils.JsonUtil.Implicits._
import io.klaytn.utils.klaytn.NumberConverter._
import io.klaytn.utils.klaytn.TransferDataReadUtil
import io.klaytn.utils.{JsonUtil, SlackUtil}

object EventLogType extends Enumeration {
  type EventLogType
  val TokenTransfer = Value("TokenTransfer")
  val NTFTransfer = Value("NTFTransfer")
  val Approval = Value("Approval")
  val ApprovalForAll = Value("ApprovalForAll")
  val TransferSingle = Value("TransferSingle")
  val TransferBatch = Value("TransferBatch")
  val MinterAdded = Value("MinterAdded")
  val AddrChanged = Value("AddrChanged")
  val NewResolver = Value("NewResolver")
  val NameChanged = Value("NameChanged")
  val Upgraded = Value("Upgraded")
  val Unknown = Value("Unknown")
}

object EventLog {
  // KIP7, KIP17
  val Transfer =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

  // KIP7, KIP17
  val Approval =
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

  // KIP7, KIP17, KIP37
  val ApprovalForAll =
    "0x17307eab39ab6107e8899845ad3d59bd9653f200f220920489ca2b5937696c31"

  // KIP7, KIP17, KIP37
  val Pause =
    "0x62e78cea01bee320cd4e420270b5ea74000d11b0c9f74754ebdbfc544b05a258"
  val UnPause =
    "0x5db9ee0a495bf2e6ff9c91a7834c1ba4fdd244a5e8aa4e537bd38aeae4b073aa"

  // KIP37
  val TransferSingle =
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
  val TransferBatch =
    "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
  // https://www.klaytnfinder.io/tx/0x383037d5a96c4ce74e798b4ce2e858202e2d09e57cc23460d38d3ed5b842c907?tabId=eventLog
  val URI = "0x6bb7ff708619ba0610cba295a58592e0451dee2622938c8755667688daf3529b" // URI(string,uint256)
  val SetTokenURI =
    "0xaa425fdd80303549e5f891d43e81f503f03bc88d66e218ac44f385682ce6fe0b" // SetTokenURI(uint256,string,string)

  // KIP7, KIP17, KIP37
  val MinterAdded =
    "0x6ae172837ea30b801fbfcdd4108aa1d5bf8ff775444fd70256b44e6bf3dfc3f6"
  val MinterRemoved =
    "0xe94479a9f7e1952cc78f2d6baab678adc1b772d936c6583def489e524cb66692"
  val PauserAdded =
    "0x6719d08c1888103bea251a4ed56406bd0c3e69723c8a1686e017e7bbe159b6f8"
  val PauserRemoved =
    "0xcd265ebaf09df2871cc7bd4133404a235ba12eff2041bb89d9c714a2621c7c7e"

  val Withdraw1 =
    "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65" // Withdrawal(address,uint256) - topic1
  val Withdraw2 =
    "0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568" // Withdraw(address,uint256,uint256) - topic1
  val Withdraw3 =
    "0x884edad9ce6fa2440d8a54cc123490eb96d2768479d49ff9c7366125a9424364" // Withdraw(address,uint256) - data parsing
  val Withdraw4 =
    "0x56c54ba9bd38d8fd62012e42c7ee564519b09763c426d331b3661b537ead19b2" // Withdraw(address,uint256,address) - The address is the contract, and where is the user's?? About 350,000...
  val Withdraw5 =
    "0xe08737ac48a1dab4b1a46c7dc9398bd5bfc6d7ad6fabb7cd8caa254de14def35" // Withdraw(address,uint256,uint256,uint256,uint256)
  val Withdraw6 =
    "0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5" // Withdrawn(address,uint256)
  val Withdraw7 =
    "0x92ccf450a286a957af52509bc1c9939d1a6a481783e142e41e2499f0bb66ebc6" // Withdrawn(address,uint256,uint256)
  val Withdraw8 =
    "0x71ef96c43343734b1d843bb85d52ef329f5e9143e4d35827771e3b0dd90c5f84" // Withdraw(uint256,uint256,address)
  val Withdraw9 =
    "0x8166bf25f8a2b7ed3c85049207da4358d16edbed977d23fa2ee6f0dde3ec2132" // Withdraw(address,uint256,uint256,address)
  val Withdraw10 =
    "0x48f1e4b8fb0469595617480e6784e40ab6b8c49209761b8e09305bf7b73e53de" // WithdrawReward(address,uint256,uint256)
//  val Withdraw11 = "0xf341246adaac6f497bc2a656f546ab9e182111d630394f0c57c710a59a2cb567" // Withdraw(address,address,uint256,uint256)
//  val Withdraw12 = "0xa866edf1861d39a9973d7892f3869e1bb4b76bc00d216872c1dd2bc547f2da2e" // Withdraw(string,bytes,bytes,bytes,bytes32[],uint256[],bytes)
//  val Withdraw13 = "0xe06fab35f3c220725c11d544884aff93ebf67222c8310c487c71f27c844593e8" // RewardWithdrawn(address,address,uint256
//  val Withdraw14 = "0xad9ab9ee6953d4d177f4a03b3a3ac3178ffcb9816319f348060194aa76b14486" // LogWithdraw(address,address,address,uint256,uint256)

  val Deposit1 =
    "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c" // Deposit(address,uint256)
  val Deposit2 =
    "0xd68606c673aecac7ce24ec44fd7d77b401356dfd76fe9b36cb58e579c4220aed" // Deposit(uint256,uint256,address)
  val Deposit3 =
    "0x90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15" // Deposit(address,uint256,uint256)
  val Deposit4 =
    "0xe31c7b8d08ee7db0afa68782e1028ef92305caeea8626633ad44d413e30f6b2f" // Deposit(address,uint256,address)
  val Deposit5 =
    "0x2da466a7b24304f47e87fa2e1e5a81b9831ce54fec19055ce277ca2f39ba42c4" // Deposited(address,uint256)
  val Deposit6 =
    "0x7162984403f6c73c8639375d45a9187dfd04602231bd8e587c415718b5f7e5f9" // Deposit(address,uint256,uint256,uint256,uint256)
  val Deposit7 =
    "0x7cfff908a4b583f36430b25d75964c458d8ede8a99bd61be750e97ee1b2f3a96" // Deposit(address,address,address,uint256)
  val Deposit8 =
    "0x02d7e648dd130fc184d383e55bb126ac4c9c60e8f94bf05acdf557ba2d540b47" // Deposit(address,uint256,uint256,address)
//  val Deposit9  = "0xa6103c513fe87f7876c848403a612d7790465e2531fed80df0c74dae035ce880" // Deposit(string,address,bytes,address,uint8,uint256,uint256,bytes)
  val Deposit10 =
    "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7" // Deposit(address,address,uint256,uint256)
  val Deposit11 =
    "0xde6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951" // Deposit(address,address,address,uint256,uint16)
  val Deposit12 =
    "0x5548c837ab068cf56a2c2479df0882a4922fd203edb7517321831d95078c5f62" // Deposit(address,address,uint256)
//  val Deposit13 = "0xa3af609bf46297028ce551832669030f9effef2b02606d02cbbcc40fe6b47c55" // Deposit(uint256,uint256)
  val Deposit14 =
    "0x4566dfc29f6f11d13a418c26a02bef7c28bae749d4de47e4e6a7cddea6730d59" // Deposit(address,uint256,uint256,int128,uint256)

  val Mint1 =
    "0x2f00e3cdd69a77be7ed215ec7b2a36784dd158f921fca79ac29deffa353fe6ee" // Mint(address,address,uint256,uint256)
  val Mints = Set(Mint1)

  val Burn1 =
    "0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5" // Burn(address,uint256)
  val Burn2 =
    "0x49995e5dd6158cf69ad3e9777c46755a1a826a446c6416992167462dad033b2a" // Burn(address,uint256,uint256)
  val Burns = Set(Burn1, Burn2)

  val Initialized =
    "0x40251fbfb6656cfa65a00d7879029fec1fad21d28fdcff2f4f68f52795b74f2c"

  val OwnershipTransferred =
    "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0"

  // kns
  val AddrChanged =
    "0x52d7d861f09ab3d26239d492e8968629f95e9e318cf0b73bfddc441522a15fd2" // AddrChanged(bytes32 indexed node, address addr)
  val NewResolver =
    "0x335721b01866dc23fbee8b6b2c7b1e14d6f05c28cd35a2c934239f94095602a0" // NewResolver(bytes32 indexed node, address resolver)
  val NameChanged =
    "0xb7d29e911041e8d9b843369e890bcb72c9388692ba48b65ac54e7214c4c348f7" // NameChanged(bytes32 indexed node, string name)

  // upgrade
  val Upgraded =
    "0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b" //Upgraded(address)
}

abstract class EventLogTypeManager(topics: Seq[String]) {
  def getSignature: String = {
    if (topics.isEmpty) ""
    else topics.head
  }

  def getType: EventLogType.Value = {
    if (topics.isEmpty) {
      return EventLogType.Unknown
    }

    topics.head match {
      case "0xdfe92792f0367a3ba3758bc05e2c4f2c18cb9d65" =>
        EventLogType.NTFTransfer
      case EventLog.Transfer =>
        topics.size match {
          case 3 => EventLogType.TokenTransfer
          case 4 => EventLogType.NTFTransfer
          case _ => EventLogType.Unknown
        }
      case EventLog.Approval       => EventLogType.Approval
      case EventLog.ApprovalForAll => EventLogType.ApprovalForAll
      case EventLog.TransferSingle => EventLogType.TransferSingle
      case EventLog.TransferBatch  => EventLogType.TransferBatch
      case EventLog.MinterAdded    => EventLogType.MinterAdded
      case EventLog.AddrChanged    => EventLogType.AddrChanged
      case EventLog.NewResolver    => EventLogType.NewResolver
      case EventLog.NameChanged    => EventLogType.NameChanged
      case EventLog.Upgraded       => EventLogType.Upgraded
      case _                       => EventLogType.Unknown
    }
  }

  def isTransferEvent: Boolean = isTransferEvent(getType)

  def isTokenTransferEvent: Boolean = getType == EventLogType.TokenTransfer

  def isMinterAdded: Boolean = getType == EventLogType.MinterAdded

  def isNFTTransferEvent: Boolean = {
    val eventType = getType
    eventType == EventLogType.NTFTransfer || eventType == EventLogType.TransferSingle || eventType == EventLogType.TransferBatch
  }

  def isTransferEvent(logType: EventLogType.Value): Boolean =
    logType == EventLogType.TokenTransfer || logType == EventLogType.NTFTransfer || logType == EventLogType.TransferSingle || logType == EventLogType.TransferBatch

  def isMintOrBurn: Boolean = {
    val signature = getSignature
    EventLog.Mints.contains(signature) || EventLog.Burns.contains(signature)
  }

  def isWithdrawType1: Boolean = getSignature == EventLog.Withdraw1

  def isDepositType1: Boolean = getSignature == EventLog.Deposit1

  def isURI: Boolean = getSignature == EventLog.URI

  def isSetTokenURI: Boolean = getSignature == EventLog.SetTokenURI

  def isApproval: Boolean = {
    val eventType = getType
    eventType == EventLogType.Approval || eventType == EventLogType.ApprovalForAll
  }

  def isUpgraded: Boolean = getType == EventLogType.Upgraded
}

case class EventLog(blockHash: String,
                    blockNumber: String,
                    transactionHash: String,
                    transactionIndex: String,
                    address: String,
                    topics: List[String],
                    data: String,
                    logIndex: String,
                    removed: Option[Boolean])
    extends EventLogTypeManager(topics) {
  def toRefined(blockNumberPartition: String,
                transactionStatus: Boolean,
                ts: Int): RefinedEventLog = {
    RefinedEventLog(
      "event_logs",
      blockNumberPartition,
      this.blockHash,
      this.blockNumber.hexToLong(),
      this.transactionHash,
      this.transactionIndex.hexToInt(),
      transactionStatus,
      this.address,
      this.topics,
      this.data,
      this.logIndex.hexToInt(),
      ts,
      removed
    )
  }
}

case class RefinedEventLog(label: String,
                           bnp: String,
                           blockHash: String,
                           blockNumber: Long,
                           transactionHash: String,
                           transactionIndex: Int,
                           transactionStatus: Boolean,
                           address: String,
                           topics: Seq[String],
                           data: String,
                           logIndex: Int,
                           timestamp: Int,
                           removed: Option[Boolean])
    extends EventLogTypeManager(topics) {

  def extractTransferInfo()
    : (String, String, String, Seq[BigInt], Seq[String]) = {
    val (from, to, tokenIds, values) = getType match {
      case TokenTransfer =>
        val (from, to) = extractFromTo(1, 2)
        (from, to, Seq(), Seq(data))
      case NTFTransfer =>
        val (from, to) = extractFromTo(1, 2)
        val tokenId = topics(3).hexToBigInt()
        (from,
         to,
         Seq(tokenId),
         Seq(
           "0x0000000000000000000000000000000000000000000000000000000000000001"))
      case TransferSingle =>
        if (topics.length >= 4) {
          val (from, to) = extractFromTo(2, 3)
          val (tokenId, value) =
            TransferDataReadUtil.decodeTransferSingleData(data)
          (from, to, Seq(tokenId), Seq(value))
        } else {
          try {
            val (_, from, to, id, value) =
              TransferDataReadUtil.decodeTransferSingleDataV2(data)
            (from, to, Seq(id), Seq(value))
          } catch {
            case _: Throwable =>
              SlackUtil.sendMessage(
                s"TransferSingle parse error: blockNumber($blockNumber), txHash($transactionHash)")
              ("", "", Seq(), Seq())
          }
        }
      case TransferBatch =>
        if (topics.length >= 4) {
          val (from, to) = extractFromTo(2, 3)
          val (tokenIds, values) =
            TransferDataReadUtil.readTransferBatchIDsAndValues(data)
          (from, to, tokenIds, values)
        } else {
          try {
            val (_, from, to, tokenIds, values) =
              TransferDataReadUtil.readTransferBatchIDsAndValuesV2(data)
            (from, to, tokenIds, values)
          } catch {
            case _: Throwable =>
              SlackUtil.sendMessage(
                s"TransferBatch parse error: blockNumber($blockNumber), txHash($transactionHash)")
              ("", "", Seq(), Seq())
          }
        }
      case _ => ("", "", Seq(), Seq())
    }

    (address, from, to, tokenIds, values)
  }

  protected def extractFromTo(fromIndex: Int,
                              toIndex: Int): (String, String) = {
    ("0x" + topics(fromIndex).substring(26),
     "0x" + topics(toIndex).substring(26))
  }
}

object RefinedEventLog {
  def parse(jsonBlock: String): Option[RefinedEventLog] = {
    JsonUtil.fromJson[RefinedEventLog](jsonBlock)
  }
}
