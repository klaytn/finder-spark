package io.klaytn.contract.lib

object ContractType extends Enumeration {
  type ContractType = Value
  protected case class Val(interfaceId: String, signatures: Map[String, String])
      extends super.Val

  import scala.language.implicitConversions
  implicit def convert(x: Value): Val = x.asInstanceOf[Val]

  /**
    * https://kips.klaytn.com/KIPs/kip-7
    * KIP7 interfaces
    * function totalSupply() external view returns (uint256);
    * function balanceOf(address account) external view returns (uint256);
    * function transfer(address recipient, uint256 amount) external returns (bool);
    * function allowance(address owner, address spender) external view returns (uint256);
    * function approve(address spender, uint256 amount) external returns (bool);
    * function transferFrom(address sender, address recipient, uint256 amount) external returns (bool);
    * function safeTransfer(address recipient, uint256 amount, bytes data) external;
    * function safeTransfer(address recipient, uint256 amount) external;
    * function safeTransferFrom(address sender, address recipient, uint256 amount, bytes data) external;
    * function safeTransferFrom(address sender, address recipient, uint256 amount) external;
    *
    * KIP7 Metadata Interfaces: Optional
    * function name() external view returns (string memory);
    * function symbol() external view returns (string memory);
    * function decimals() external view returns (uint8);
    */
  val KIP7: Val = Val(
    "0x65787371",
    Map(
      "18160ddd7f15c72528c2f94fd8dfe3c8d5aa26e2c50c7d81f4bc7bee8d4b7932" -> "totalSupply()",
      "70a08231b98ef4ca268c9cc3f6b4590e4bfec28280db06bb5d45e689f2a360be" -> "balanceOf(address)",
      "a9059cbb2ab09eb219583f4a59a5d0623ade346d962bcd4e46b11da047c9049b" -> "transfer(address,uint256)",
      "dd62ed3e90e97b3d417db9c0c7522647811bafca5afc6694f143588d255fdfb4" -> "allowance(address,address)",
      "095ea7b334ae44009aa867bfb386f5c3b4b443ac6f0ee573fa91c4608fbadfba" -> "approve(address,uint256)",
      "23b872dd7302113369cda2901243429419bec145408fa8b352b3dd92b66c680b" -> "transferFrom(address,address,uint256)",
      "eb7955494511a2d9a131ce5ddaa7623a542d377016a10a6e91d96372e1f940e7" -> "safeTransfer(address,uint256,bytes)",
      "423f6ceff4945dae9982043aeaaa47692a25db9a36e5f4050e61f30b1d67e9c8" -> "safeTransfer(address,uint256)",
      "b88d4fde60196325a28bb7f99a2582e0b46de55b18761e960c14ad7a32099465" -> "safeTransferFrom(address,address,uint256,bytes)",
      "42842e0eb38857a7775b4e7364b2775df7325074d088e7fb39590cd6281184ed" -> "safeTransferFrom(address,address,uint256)",
//      "06fdde0383f15d582d1a74511486c9ddf862a882fb7904b3d9fe9b8b8e58a796" -> "name()",
//      "95d89b41e2f5f391a79ec54e9d87c79d6e777c63e32c28da95b4e9e4a79250ec" -> "symbol()",
//      "313ce567add4d438edf58b94ff345d7d38c45b17dfc0f947988d7819dca364f9" -> "decimals()"
    )
  )

  /**
    * https://kips.klaytn.com/KIPs/kip-17
    * KIP17 interfaces
    * function balanceOf(address _owner) external view returns (uint256);
    * function ownerOf(uint256 _tokenId) external view returns (address);
    * function safeTransferFrom(address _from, address _to, uint256 _tokenId, bytes _data) external payable;
    * function safeTransferFrom(address _from, address _to, uint256 _tokenId) external payable;
    * function transferFrom(address _from, address _to, uint256 _tokenId) external payable;
    * function approve(address _approved, uint256 _tokenId) external payable;
    * function setApprovalForAll(address _operator, bool _approved) external;
    * function getApproved(uint256 _tokenId) external view returns (address);
    * function isApprovedForAll(address _owner, address _operator) external view returns (bool);
    *
    * KIP17 Metadata interfaces
    * function name() external view returns (string _name);
    * function symbol() external view returns (string _symbol);
    * function tokenURI(uint256 _tokenId) external view returns (string);
    */
  val KIP17: Val = Val(
    "0x80ac58cd",
    Map(
      "70a08231b98ef4ca268c9cc3f6b4590e4bfec28280db06bb5d45e689f2a360be" -> "balanceOf(address)",
      "6352211e6566aa027e75ac9dbf2423197fbd9b82b9d981a3ab367d355866aa1c" -> "ownerOf(uint256)",
      "b88d4fde60196325a28bb7f99a2582e0b46de55b18761e960c14ad7a32099465" -> "safeTransferFrom(address,address,uint256,bytes)",
      "42842e0eb38857a7775b4e7364b2775df7325074d088e7fb39590cd6281184ed" -> "safeTransferFrom(address,address,uint256)",
      "23b872dd7302113369cda2901243429419bec145408fa8b352b3dd92b66c680b" -> "transferFrom(address,address,uint256)",
      "095ea7b334ae44009aa867bfb386f5c3b4b443ac6f0ee573fa91c4608fbadfba" -> "approve(address,uint256)",
      "a22cb4651ab9570f89bb516380c40ce76762284fb1f21337ceaf6adab99e7d4a" -> "setApprovalForAll(address,bool)",
      "081812fc55e34fdc7cf5d8b5cf4e3621fa6423fde952ec6ab24afdc0d85c0b2e" -> "getApproved(uint256)",
      "e985e9c5c6636c6879256001057b28ccac7718ef0ac56553ff9b926452cab8a3" -> "isApprovedForAll(address,address)",
      "06fdde0383f15d582d1a74511486c9ddf862a882fb7904b3d9fe9b8b8e58a796" -> "name()",
      "95d89b41e2f5f391a79ec54e9d87c79d6e777c63e32c28da95b4e9e4a79250ec" -> "symbol()",
      "c87b56dda752230262935940d907f047a9f86bb5ee6aa33511fc86db33fea6cc" -> "tokenURI(uint256)"
    )
  )

  /**
    * https://kips.klaytn.com/KIPs/kip-37
    * function safeTransferFrom(address _from, address _to, uint256 _id, uint256 _value, bytes calldata _data) external;
    * function safeBatchTransferFrom(address _from, address _to, uint256[] calldata _ids, uint256[] calldata _values, bytes calldata _data) external;
    * function balanceOf(address _owner, uint256 _id) external view returns (uint256);
    * function balanceOfBatch(address[] calldata _owners, uint256[] calldata _ids) external view returns (uint256[] memory);
    * function setApprovalForAll(address _operator, bool _approved) external;
    * function isApprovedForAll(address _owner, address _operator) external view returns (bool);
    * function totalSupply(uint256 _id) external view returns (uint256);
    *
    * KIP37 Metadata URI interfaces
    * function uri(uint256 _id) external view returns (string memory);
    */
  val KIP37: Val = Val(
    "0x6433ca1f",
    Map(
      "f242432a01954b0e0efb67e72c9b3b8ed77690657780385b256ac9aba0e35f0b" -> "safeTransferFrom(address,address,uint256,uint256,bytes)",
      "2eb2c2d667cf048e1ec82a4537b123e459951729e09ec737ac88a8e82fb2d1db" -> "safeBatchTransferFrom(address,address,uint256[],uint256[],bytes)",
      "00fdd58ea0325fd79f486f8008ad3fad17dcb1cd2ee8474215c114771d87863e" -> "balanceOf(address,uint256)",
      "4e1273f46c229233ffb464c9596131917915848124c0e2e01ddcbd310b2609d4" -> "balanceOfBatch(address[],uint256[])",
      "a22cb4651ab9570f89bb516380c40ce76762284fb1f21337ceaf6adab99e7d4a" -> "setApprovalForAll(address,bool)",
      "e985e9c5c6636c6879256001057b28ccac7718ef0ac56553ff9b926452cab8a3" -> "isApprovedForAll(address,address)",
      "bd85b039b9a7f37098d2e4b53d09e5f30943e1b957ac0be596d2535b0f8e8abe" -> "totalSupply(uint256)",
      "0e89341c5b7431e95282621bb9c54e51fb5c29234df43f9e19151d3892fb0380" -> "uri(uint256)"
    )
  )

  /**
    * https://eips.ethereum.org/EIPS/eip-20
    * function name() public view returns (string): Optional
    * function symbol() public view returns (string): Optional
    * function totalSupply() public view returns (uint256)
    * function balanceOf(address _owner) public view returns (uint256 balance)
    * function transfer(address _to, uint256 _value) public returns (bool success)
    * function transferFrom(address _from, address _to, uint256 _value) public returns (bool success)
    * function approve(address _spender, uint256 _value) public returns (bool success)
    * function allowance(address _owner, address _spender) public view returns (uint256 remaining)
    */
  val ERC20: Val = Val(
    "0x36372b07",
    Map(
//      "06fdde0383f15d582d1a74511486c9ddf862a882fb7904b3d9fe9b8b8e58a796" -> "name()",
//      "95d89b41e2f5f391a79ec54e9d87c79d6e777c63e32c28da95b4e9e4a79250ec" -> "symbol()",
      "18160ddd7f15c72528c2f94fd8dfe3c8d5aa26e2c50c7d81f4bc7bee8d4b7932" -> "totalSupply()",
      "70a08231b98ef4ca268c9cc3f6b4590e4bfec28280db06bb5d45e689f2a360be" -> "balanceOf(address)",
      "a9059cbb2ab09eb219583f4a59a5d0623ade346d962bcd4e46b11da047c9049b" -> "transfer(address,uint256)",
      "23b872dd7302113369cda2901243429419bec145408fa8b352b3dd92b66c680b" -> "transferFrom(address,address,uint256)",
      "095ea7b334ae44009aa867bfb386f5c3b4b443ac6f0ee573fa91c4608fbadfba" -> "approve(address,uint256)",
      "dd62ed3e90e97b3d417db9c0c7522647811bafca5afc6694f143588d255fdfb4" -> "allowance(address,address)"
    )
  )

  /**
    * https://eips.ethereum.org/EIPS/eip-721
    */
  val ERC721: Val = Val(
    "0x80ac58cd",
    Map(
      "70a08231b98ef4ca268c9cc3f6b4590e4bfec28280db06bb5d45e689f2a360be" -> "balanceOf(address)",
      "6352211e6566aa027e75ac9dbf2423197fbd9b82b9d981a3ab367d355866aa1c" -> "ownerOf(uint256)",
      "b88d4fde60196325a28bb7f99a2582e0b46de55b18761e960c14ad7a32099465" -> "safeTransferFrom(address,address,uint256,bytes)",
      "42842e0eb38857a7775b4e7364b2775df7325074d088e7fb39590cd6281184ed" -> "safeTransferFrom(address,address,uint256)",
      "23b872dd7302113369cda2901243429419bec145408fa8b352b3dd92b66c680b" -> "transferFrom(address,address,uint256)",
      "095ea7b334ae44009aa867bfb386f5c3b4b443ac6f0ee573fa91c4608fbadfba" -> "approve(address,uint256)",
      "a22cb4651ab9570f89bb516380c40ce76762284fb1f21337ceaf6adab99e7d4a" -> "setApprovalForAll(address,bool)",
      "081812fc55e34fdc7cf5d8b5cf4e3621fa6423fde952ec6ab24afdc0d85c0b2e" -> "getApproved(uint256)",
      "e985e9c5c6636c6879256001057b28ccac7718ef0ac56553ff9b926452cab8a3" -> "isApprovedForAll(address,address)",
      "06fdde0383f15d582d1a74511486c9ddf862a882fb7904b3d9fe9b8b8e58a796" -> "name()",
      "95d89b41e2f5f391a79ec54e9d87c79d6e777c63e32c28da95b4e9e4a79250ec" -> "symbol()",
      "c87b56dda752230262935940d907f047a9f86bb5ee6aa33511fc86db33fea6cc" -> "tokenURI(uint256)"
    )
  )

  /**
    * https://eips.ethereum.org/EIPS/eip-1155
    */
  val ERC1155: Val = Val(
    "0xd9b67a26",
    Map(
      "f242432a01954b0e0efb67e72c9b3b8ed77690657780385b256ac9aba0e35f0b" -> "safeTransferFrom(address,address,uint256,uint256,bytes)",
      "2eb2c2d667cf048e1ec82a4537b123e459951729e09ec737ac88a8e82fb2d1db" -> "safeBatchTransferFrom(address,address,uint256[],uint256[],bytes)",
      "00fdd58ea0325fd79f486f8008ad3fad17dcb1cd2ee8474215c114771d87863e" -> "balanceOf(address,uint256)",
      "4e1273f46c229233ffb464c9596131917915848124c0e2e01ddcbd310b2609d4" -> "balanceOfBatch(address[],uint256[])",
      "a22cb4651ab9570f89bb516380c40ce76762284fb1f21337ceaf6adab99e7d4a" -> "setApprovalForAll(address,bool)",
      "e985e9c5c6636c6879256001057b28ccac7718ef0ac56553ff9b926452cab8a3" -> "isApprovedForAll(address,address)",
      "0e89341c5b7431e95282621bb9c54e51fb5c29234df43f9e19151d3892fb0380" -> "uri(uint256)"
    )
  )
}
