# Batch Execution

The overall flow is as follows:

1. Kafka Log (Data) Generation

- Generate through EN NODE by streaming DumpKafkaBlock, DumpKafkaInternalTx
- Alternatively, download Kafka logs and upload to S3
- 1 BNP batch execution up to a specific BNP (1 BNP = 10,000 blocks)
- Resume streaming, where BNP at the point of resumption is referred to as n
- Execute batches from 1260 BNP to n BNP
- Data integrity verification

The following provides an explanation of the batches.

## 1. Kafka Log

- Collect Kafka logs through DumpKafkaBlock streaming, DumpKafkaInternalTx streaming
- Kafka logs are necessary for recovery processing
- Kafka log storage can be configured with `S3_BUCKET` in `prod.env.sh`, `dev.env.sh`
- Stored in `s3://klaytn-prod-lake/klaytn/cypress/label=kafka_log/`

## 2. Processed Data Storage Location

- Temporary storage on S3, then saved via bulk load from RDS
- Configurable in `all.baobab.conf`, `all.cypress.conf` via `spark.app.base.bucket`
- Stored in `/output/` path in the storage specified by `spark.app.base.bucket`

## 3. Batch Order and Description

### a. BlockMakeData

- Read kafka_log and create Block data
- Created data is stored in locations `klaytn-prod-spark/prod-cypress/loadDataFromS3/block`, `klaytn-prod-spark/prod-cypress/loadDataFromS3/eventlog`, `klaytn-prod-spark/prod-cypress/loadDataFromS3/tx`
- Corresponding data paths are stored in location `klaytn-prod-spark/prod-cypress/loadDataFromS3/list/block`

### b. InternalTxMakeData

- Read kafka_log and create InternalTx data
- Created data is stored in locations `klaytn-prod-spark/prod-cypress/loadDataFromS3/trace`, `klaytn-prod-spark/prod-cypress/loadDataFromS3/trace_index`
- Corresponding data paths are stored in location `klaytn-prod-spark/prod-cypress/loadDataFromS3/list/trace`

### c. AccountMakeData

- Read kafka_log and create Account data
- Created data is stored in location `klaytn-prod-spark/prod-cypress/AccountBatch`

### d. BlockLoadData

- Bulk load data created in step a to RDS
- Stored in tables `blocks`, `event_logs`, `transactions`

### e. InternalTxLoadData

- Bulk load data created in step b to RDS
- Stored in tables `internal_transactions`, `internal_transaction_index`

### f. AccountLoadData

- Bulk load data created in step c to RDS
- Stored in tables `accounts`, `contracts`

### g. TransferBatch

- Read kafka_log and create Transfer data
- Stored in tables `token_transfer`, `nft_transfer`, `account_transfer_contract`

### h. ApproveBurnBatch

- Read kafka_log and create NFT, Token-related Approve, Burn data
- Stored in tables `account_token_approves`, `account_nft_approves`, `token_burns`, `nft_burns`

### i. BlockBurnBatch, LoadBlockRewardBatch

- Read kafka_log and create BlockBurn data
- Stored in table `block_burns`
- Calculate block reward and store in table `block_rewards` (LoadBlockRewardBatch)

### j. HolderBatch, CounterBatch, MinusHolderBatch

- Read kafka_log and create `token_holders` table
- Read kafka_log and create `nft_holders`, `nft_inventories` tables
- Calculate holder, burn, total Transfer, burn transfer, total supply, etc., update `contracts` table (CounterBatch)
- If the number of holders becomes negative, perform adjustments (MinusHolderBatch)

### k. MigrateBatch (Optional)

- Retrieve data from the existing database and update the new database
- The following items are updated:
- accounts
  - tag, gc, address_label
- klaytn_name_service
- contracts
  - icon, official_site, email, verified, name

### l. AccountESRecoveryBatch, TransactionESRecoveryBatch, ContractESRecoveryBatch

- Based on account, tx, contract information stored in the DB, send bulk indexing requests to OpenSearch

## 4. Batches to Run for Each Table

### Account / Contract

- account_keys: AccountMakeData -> AccountLoadData
- accounts: AccountMakeData -> AccountLoadData
- contracts: AccountMakeData -> AccountLoadData

### Block / Tx / Eventlog

- blocks: BlockMakeData -> BlockLoadData
- event_logs: BlockMakeData -> BlockLoadData
- transactions: BlockMakeData -> BlockLoadData

### InternalTx

- internal_transactions: InternalTxMakeData -> InternalTxLoadData
- internal_transaction_indexes: InternalTxMakeData -> InternalTxLoadData

## BlockBurn, BlockReward

- block_burns: BlockBurnBatch
- block_rewards: LoadBlockRewardBatch

### NFT, Token (transfers, approves, burns)

- token_transfers: TransferBatch
- nft_transfers: TransferBatch
- account_token_approves: ApproveBurnBatch
- account_nft_approves: ApproveBurnBatch
- token_burns: Appro

### NFT, Token (holders, inventories)

- token_holders: HolderBatch -> MinusHolderBatch
- nft_holders: HolderBatch -> MinusHolderBatch
- nft_inventories: HolderBatch

### Manually Created

- klaytn_name_service
- governance_councils
- governance_council_contracts
- function_signatures
- event_signatures
- users
- nft_patterned_uri
- nft_aggregate_flag
- contract_submission_requests
- contract_codes
