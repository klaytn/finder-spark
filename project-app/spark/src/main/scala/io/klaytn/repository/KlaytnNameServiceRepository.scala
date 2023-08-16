package io.klaytn.repository

import io.klaytn.dsl.db.withDB

case class KlaytnNameService(name: String,
                             resolvedAddress: String,
                             resolverAddress: String,
                             nameHash: String,
                             tokenId: BigInt)

class KlaytnNameServiceRepository extends AbstractRepository {
  val DB: String = "finder0101"
  val Table = "klaytn_name_service"

  def insert(klaytnNameService: KlaytnNameService): Unit = {
    withDB(DB) { c =>
      val pstmt = c.prepareStatement(
        s"INSERT INTO $Table (`name`,`resolved_address`,`resolver_address`,`name_hash`,`token_id`) VALUES (?,?,?,?,?)")

      pstmt.setString(1, klaytnNameService.name)
      pstmt.setString(2, klaytnNameService.resolvedAddress)
      pstmt.setString(3, klaytnNameService.resolverAddress)
      pstmt.setString(4, klaytnNameService.nameHash)
      pstmt.setString(5, klaytnNameService.tokenId.toString)
      pstmt.execute()

      pstmt.close()
    }
  }

  def updateResolverAddress(nameHash: String, resolverAddress: String): Unit = {
    withDB(DB) { c =>
      val pstmt = c.prepareStatement(
        s"UPDATE $Table SET `resolver_address`=? WHERE `name_hash`=?")

      pstmt.setString(1, resolverAddress)
      pstmt.setString(2, nameHash)
      pstmt.execute()

      pstmt.close()
    }
  }

  def updateResolvedAddress(nameHash: String, resolvedAddress: String): Unit = {
    withDB(DB) { c =>
      val pstmt = c.prepareStatement(
        s"UPDATE $Table SET `resolved_address`=? WHERE `name_hash`=?")

      pstmt.setString(1, resolvedAddress)
      pstmt.setString(2, nameHash)
      pstmt.execute()

      pstmt.close()
    }
  }

  def updateAddresses(nameHash: String,
                      resolvedAddress: String,
                      resolverAddress: String): Unit = {
    withDB(DB) { c =>
      val pstmt =
        c.prepareStatement(
          s"UPDATE $Table SET `resolved_address`=?, `resolver_address`=? WHERE `name_hash`=?")

      pstmt.setString(1, resolvedAddress)
      pstmt.setString(2, resolverAddress)
      pstmt.setString(3, nameHash)
      pstmt.execute()

      pstmt.close()
    }
  }

  def getKlaytnNameService(whereField: String,
                           value: String): Option[KlaytnNameService] = {
    withDB(DB) { c =>
      val pstmt = c.prepareStatement(
        s"SELECT `name`,`resolved_address`,`resolver_address`,`name_hash`,`token_id` FROM $Table WHERE `$whereField`=?")
      pstmt.setString(1, value)

      val rs = pstmt.executeQuery()
      val result = if (rs.next()) {
        KlaytnNameService(rs.getString(1),
                          rs.getString(2),
                          rs.getString(3),
                          rs.getString(4),
                          BigInt(rs.getString(5)))
      } else {
        null
      }

      rs.close()
      pstmt.close()

      Option(result)
    }
  }

  def getKlaytnNameServiceByName(name: String): Option[KlaytnNameService] =
    getKlaytnNameService("name", name)

  def getKlaytnNameServiceByNameHash(
      nameHash: String): Option[KlaytnNameService] =
    getKlaytnNameService("name_hash", nameHash)
}
