# klaytn-spark

This repository contains the Spark application code for the Klaytn blockchain data processing.

For the overall architecture of the finder, please refer to the [Main Repo](https://github.com/klaytn/finder/blob/main/README.md).

---

## Overview

This README contains

- **Build**
- **Test / Execution**
- **Deployment**
- **Monitoring**

For the details of the application, please refer to the following READMEs.

- **STREAMING PROCESS**: https://github.com/klaytn/finder-spark/blob/main/project-app/spark/src/main/scala/io/klaytn/apps/README.md

- **BATCH PROCESS**: https://github.com/klaytn/finder-spark/blob/main/project-app/spark/src/main/scala/io/klaytn/apps/restore/README.md

--

## Build

`sbt klaytn-spark/clean klaytn-spark/assembly`

## Test / Execution

- Using Spark: configure `/bin/local_submit.sh` and run in the local
- Without using Spark: Use sbt test
- Running tests locally for the Main program execution: Implement in the test directory with a name like -TestMain

You can run the test with the following command.

> `sbt test`

If you want to run a specific test, you can run it with the following command.

> `sbt testOnly io.klaytn.contract.lib.ERC20MetadataReaderTestMain`

---

## Deployment

### Prerequisites

1. **Login AWS CLI / aws-vault**
   - > `aws-vault exec YOUR_AWS_PROFILE`
2. **Load the target cluster environment**
   - > `source conf/prod.env.sh`
   - > `source conf/dev.env.sh`
3. **Copy the EMR ClusterId**
   - > `aws emr list-clusters --active | jq -r '.[] | sort_by(.Name)' | jq -r '.[] | [.Id,.Status.State,.Name] | @tsv' | awk 'BEGIN { print "——————————————— —————————————— ——————————————————————————"; print "   ClusterId        Status            ClusterName       "; print "——————————————— —————————————— ——————————————————————————" }; { printf "\033[1;32m%-15s\033[0m \033[1;36m%14s\033[0m %s\n", $1, $2, $3 };'`

---

### Run the release_v3.sh script

- **cluster_id**: EMR ClusterId to deploy
- **canonical_name**: Focus on the code you want to deploy in the Intellij project tree view, then copy it with "cmd + option + shift + c".
  - Example: io.klaytn.apps.block.BlockToDBStreaming
- **chain**: Enter the chain name corresponding to Chain.scala.
- **--build**: If this parameter exists, it will create a jar using `sbt clean assembly`. If not specified, the previously created jar will be deployed.

> `./release_v3.sh {cluster_id} {canonical_name} {chain} [--build]`

Example

- > `./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.block.BlockToDBStreaming cypress --build`

### Notes

**Before the new deployment, Please delete this file to shutdown the previous job.**

`if-want-stop-delete-this-file.prod-{baobab | cypress}.txt`

You can find the file in the following path.

`s3://klaytn-prod-spark/jobs/{canonical_name}`.

---

## Monitoring

- Structure for monitoring `io.klaytn.tools.EMRStepMonitoringRunner` in the `project-tool` through cron schedule on monitoring EC2 instances.
  - https://github.com/klaytn/finder-infra/tree/main/aws/04.data/05.emr_ec2_monitor/prod/ec2.tf
  - Cron schedule: `*/2 * * * * /bin/java -cp /home/ec2-user/klaytn-cli.jar io.klaytn.tools.EMRStepMonitoringRunner klaytn`
- How to check content from the monitoring server
  - `aws ssm start-session --target {ec2_instance_id}`
  - `sudo su`
  - `crontab -l`
  - Log check: `tail -f /var/log/cron`
- Monitoring target management
  - EMR targets are separated by tabs.
  - Managed in spreadsheets. If marked with "O" in auto-recovery, automatic recovery is performed.
  - https://docs.google.com/spreadsheets/d/1fcgGI7a8s6ujOBxDqzR0EiGVLI5cl5eP5O31a9BnlQ4/edit#gid=0

---

## Deploy Batch Examples

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.refining.RestoreKafkaLogBatch cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.refining.RestoreKafkaLogBatch baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.BlockMakeData cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.BlockMakeData baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.InternalTXMakeData cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.InternalTXMakeData baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.AccountMakeData cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.AccountMakeData baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.BlockLoadData cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.BlockLoadData baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.InternalTXLoadData cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.InternalTXLoadData baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.AccountLoadData cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.bulkload.AccountLoadData baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.approveBurn.ApproveBurnBatch cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.approveBurn.ApproveBurnBatch baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.transfer.TransferBatch cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.transfer.TransferBatch baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.adhoc.block.LoadBlockRewardBatch cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.adhoc.block.LoadBlockRewardBatch baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.holder.HolderBatch cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.holder.HolderBatch baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.holder.MinusHolderBatch cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.holder.MinusHolderBatch baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.count.CounterBatch cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.count.CounterBatch baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.es.TransactionESRecoveryBatch cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.es.TransactionESRecoveryBatch baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.es.AccountESRecoveryBatch cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.es.AccountESRecoveryBatch baobab --build

./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.es.ContractESRecoveryBatch cypress --build
./release_v3.sh j-5U3NWQOKI59S io.klaytn.apps.restore.es.ContractESRecoveryBatch baobab --build

## Deploy Streaming Examples

./release_v3.sh finder-streaming-prod io.klaytn.apps.block.BlockToDBStreaming cypress --build
./release_v3.sh finder-streaming-prod io.klaytn.apps.block.BlockToDBStreaming baobab --build

./release_v3.sh finder-streaming-prod io.klaytn.apps.itx.InternalTXToDBStreaming cypress --build
./release_v3.sh finder-streaming-prod io.klaytn.apps.itx.InternalTXToDBStreaming baobab --build

./release_v3.sh finder-streaming-prod io.klaytn.apps.account.AccountToDBStreaming cypress --build
./release_v3.sh finder-streaming-prod io.klaytn.apps.account.AccountToDBStreaming baobab --build

./release_v3.sh finder-streaming-prod io.klaytn.apps.transfer.TransferToDBStreaming cypress --build
./release_v3.sh finder-streaming-prod io.klaytn.apps.transfer.TransferToDBStreaming baobab --build

./release_v3.sh finder-streaming-prod io.klaytn.apps.worker.FastWorkerStreaming cypress --build
./release_v3.sh finder-streaming-prod io.klaytn.apps.worker.FastWorkerStreaming baobab --build

./release_v3.sh finder-streaming-prod io.klaytn.apps.worker.SlowWorkerStreaming cypress --build
./release_v3.sh finder-streaming-prod io.klaytn.apps.worker.SlowWorkerStreaming baobab --build

./release_v3.sh finder-streaming-prod io.klaytn.apps.block.BlockToESStreaming cypress --build
./release_v3.sh finder-streaming-prod io.klaytn.apps.block.BlockToESStreaming baobab --build

./release_v3.sh finder-streaming-prod io.klaytn.apps.block.DumpKafkaBlock cypress --build
./release_v3.sh finder-streaming-prod io.klaytn.apps.block.DumpKafkaBlock baobab --build

./release_v3.sh finder-streaming-prod io.klaytn.apps.itx.DumpKafkaInternalTX cypress --build
./release_v3.sh finder-streaming-prod io.klaytn.apps.itx.DumpKafkaInternalTX baobab --build
