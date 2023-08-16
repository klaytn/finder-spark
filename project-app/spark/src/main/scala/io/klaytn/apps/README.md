# Streaming Processing

## 1. BlockToDBStreaming

- Streaming processing to read block information and store it in the database
- Streaming processing to cleanse and store block, tx, event_logs in the database
- Streaming processing to calculate blockReward and store it in the database
- Table Affected: `blocks`, `transactions`, `event_logs`, `block_rewards`

## 2. InternalTXToDBStreaming

- Streaming processing to read Internal transactions and store them in the database
- Table Affected: `internal_transactions`, `internal_transaction_index` (indexing per account address)

## 3. AccountToDBStreaming

- Streaming processing to read Account information and store it in the database
- Processing for KNS, Implemented contracts
- Processing for SCA (Smart Contract Account)
- Processing for EOA (Externally Owned Account)
- Table Affected: `account_keys`, `accounts`, `contracts`
- Indexing kns in Open Search

## 4. TransferToDBStreaming

- Streaming processing to read Token (KIP-7, ERC20) Transfer, Approve, Burn information and store them in the database
- Streaming processing to read NFT (KIP-17, ERC721, KIP-37, ERC-1155) Transfer, Approve, Burn, tokenURI information and store them in the database
- Processing of holder balance based on transfer, burn events
- Table Affected: `token_transfers`, `nft_transfers`, `nft_inventories`, `token_holders`, `nft_holders`, `account_token_approves`, `account_nft_approves`

## 5. FastWorkerStreaming

- blockRestore: Streaming processing to read and compare the latest n blocks from the `blocks` db, identify missing blocks, and store them in the database
- blockBurnFee: Streaming processing to read the last block number from `block_burns`, calculate burn fee for subsequent blocks, and store them in the database
- tokenBurnAmount: Streaming processing to read token burn events, calculate burn amount for tokens, and update the `contracts` table
- nftBurnAmount: Streaming processing to read NFT burn events, calculate burn amount for NFTs, and update the `contracts` table
- tokenHolder: Streaming processing to read `token_transfers` table, add to `account_transfer_contracts` table, process holder balance, update contracts table
- nftHolder: Streaming processing to read `nft_transfers` table, add to `account_transfer_contracts` table, process tokenURI (holders, inventories processing managed by DB functions through `nft_aggregate_flag`!)
- Table Affected: `account_transfer_contracts`, `contracts`, `token_holders`, `nft_holders`, `nft_inventories`, `blocks`, `block_burns`

## 6. SlowWorkerStreaming

- stat: Streaming processing to aggregate `avg block time (24h)`, `avg tx per block`, `transaction history`, `burnt by gas fee history`, and store in `redis`
- correctHolder: Periodically verify NFT, token holders and update `nft_holders`, `token_holders` tables
- regContractFromITX: Streaming processing to read `internal_transactions` table, add to `contracts` table
- Table Affected: `contracts`, `nft_holders`, `token_holders`, `redis`

## 7. BlockToESStreaming

- Streaming processing to index `transactions` in Opensearch based on `transaction_hash`

## 8. DumpKafkaBlock

- Streaming processing to store block information received through Kafka in S3
- Kafka log storage can be configured with `S3_BUCKET` in `prod.env.sh`, `dev.env.sh`
- Stored in `s3://klaytn-prod-lake/klaytn/cypress/label=kafka_log/`

## 9. DumpKafkaInternalTX

- Streaming processing to store Internal transaction information received through Kafka in S3
- Kafka log storage can be configured with `S3_BUCKET` in `prod.env.sh`, `dev.env.sh`
- Stored in `s3://klaytn-prod-lake/klaytn/cypress/label=kafka_log/`
