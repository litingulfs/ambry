/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.cloud;

import com.github.ambry.cloud.azure.AzureCloudConfig;
import com.github.ambry.cloud.azure.AzureCloudDestinationSync;
import com.github.ambry.clustermap.AmbryDisk;
import com.github.ambry.clustermap.AmbryPartition;
import com.github.ambry.clustermap.AmbryReplica;
import com.github.ambry.clustermap.AmbryServerReplica;
import com.github.ambry.clustermap.CompositeClusterManager;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.Disk;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.HardwareState;
import com.github.ambry.clustermap.HelixClusterManager;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaSealStatus;
import com.github.ambry.clustermap.StaticClusterManager;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.ReplicationConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.BackupCheckerThread;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.replication.ReplicationManager;
import com.github.ambry.replication.ReplicationMetrics;
import com.github.ambry.store.BlobStore;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreFindToken;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * BackupIntegrityMonitor monitors the integrity of backup partitions in Azure by comparing them with server partitions.
 * It is NOT thread-safe because it clears entire disk before each run. This is the simplest thing to do rather than
 * being clever about how to manage disk space.
 *
 * We do not want multiple threads or processes of BackupIntegrityMonitor running around in data center as that will put
 * unnecessary load on Azure and on the servers. We want exactly _one_ thread of BackupIntegrityMonitor running per
 * ambry cluster in a data center.
 *
 * Here are the steps BackupIntegrityMonitor follows:
 * 0. Clear all local disks
 * 1. Randomly pick a partition P & its server-replica R from helix cluster-map
 * 2. Pick a local disk D using static cluster-map to store partition data
 * 3. Restore P from Azure to D => while(!done) { RecoveryThread::replicate(P) }
 * 4. Copy metadata from R & compare with metadata in D => while(!done) { BackupCheckerThread::replicate(R) }
 *
 * Because we clear disks in step 0, we must pick a partition randomly in step 1.
 * If we sort and pick partitions from either end of the list, we will never get to the other end of the list because
 * there will always at least be a restart due a deployment.
 */
public class BackupIntegrityMonitor implements Runnable {
  private final Logger logger = LoggerFactory.getLogger(RecoveryThread.class);
  private final ClusterMapConfig clusterMapConfig;
  private final ReplicationConfig replicationConfig;
  private final HelixClusterManager helixClusterManager;
  private final StaticClusterManager staticClusterManager;
  private final DataNodeId nodeId;
  private final RecoveryManager azureReplicationManager;
  private final RecoveryThread azureReplicator;
  private final CompositeClusterManager cluster;
  private final AzureCloudDestinationSync azureSyncClient;
  private BackupCheckerThread serverScanner;
  private final ReplicationManager serverReplicationManager;
  private final ScheduledExecutorService executor;
  private final StorageManager storageManager;
  private final AzureCloudConfig azureConfig;
  private final RecoveryMetrics metrics;
  private long currentPartitionId;
  private int currentDiskId;

  public BackupIntegrityMonitor(RecoveryManager azure, ReplicationManager server,
      CompositeClusterManager cluster, StorageManager storage, DataNodeId node,
      VerifiableProperties properties) throws ReflectiveOperationException {
    azureReplicationManager = azure;
    azureReplicator = azure.getRecoveryThread("ambry_backup_integrity_monitor");
    azureConfig = new AzureCloudConfig(properties);
    this.cluster = cluster;
    clusterMapConfig = new ClusterMapConfig(properties);
    replicationConfig = new ReplicationConfig(properties);
    helixClusterManager = cluster.getHelixClusterManager();
    staticClusterManager = cluster.getStaticClusterManager();
    executor = Utils.newScheduler(1, "ambry_backup_integrity_monitor_", true);
    nodeId = node;
    metrics = new RecoveryMetrics(cluster.getMetricRegistry());
    serverReplicationManager = server;
    storageManager = storage;
    azureSyncClient = new AzureCloudDestinationSync(properties, helixClusterManager.getMetricRegistry(),
        helixClusterManager, null);
    /**
     * After a restart, we scan the output folder to get the last partition-id verified. We don't know if
     * we crashed or restarted while scanning that partition, so we start from there. At the end, we increment the
     * currentPartitionId so that the next run() scans and verifies the next partition. This way we do not miss scanning
     * and verifying any partition. We mod and circle back to 0 once all partitions in the cluster-map have been verified.
     *
     * So the sequence looks like this:
     * R1: 0 1 2 3 x (crash/restart/deployment)
     * R2:       3 4 5 x (crash/restart/deployment)
     * R3:           5 6 7 ... so on.
     */
    currentPartitionId = getLastPartitionIdVerified();
    currentDiskId = 0;
    // log disk state
    staticClusterManager.getDataNodeId(nodeId.getHostname(), nodeId.getPort())
        .getDiskIds()
        .forEach(d -> logger.info("[BackupIntegrityMonitor] Disk = {} {} {} bytes",
            d.getMountPath(), d.getState(), d.getRawCapacityInBytes()));
    logger.info("[BackupIntegrityMonitor] Created BackupIntegrityMonitor");
  }

  /**
   * Returns the last partition-id scanned and verified
   * @return
   */
  long getLastPartitionIdVerified() {
    long maxPartitionId = 0;
    try {
      for(File file : new File(replicationConfig.backupCheckerReportDir).listFiles()) {
        maxPartitionId = Math.max(maxPartitionId, Long.parseLong(file.getName()));
      }
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error("[BackupIntegrityMonitor] Failed to get last partition-id due to {}, start verification from partition-id 0", e.getMessage());
    }
    return maxPartitionId;
  }

  class PartitionIdComparator implements Comparator<PartitionId> {
    @Override
    public int compare(PartitionId p1, PartitionId p2) {
      return p1.getId() >= p2.getId() ? 1 : -1;
    }
  }

  /**
   * Starts and schedules monitor
   */
  public void start() {
    executor.scheduleWithFixedDelay(this::run, 0, 30, TimeUnit.MINUTES);
    logger.info("[BackupIntegrityMonitor] Started BackupIntegrityMonitor");
  }

  /**
   * Shut down the monitor waiting for in progress operations to complete.
   */
  public void shutdown() {
    /*
      Shutdown executor.
      This arbitrary wait is merely an attempt to allow the worker threads to gracefully exit.
      We will force a shutdown later. All workers are daemons and JVM _will_ exit when only daemons remain.
      Any data inconsistencies must be resolved separately, but not by trying to predict the right shutdown timeout.
    */
    logger.info("[BackupIntegrityMonitor] Shutting down BackupIntegrityMonitor");
    Utils.shutDownExecutorService(executor, 10, TimeUnit.SECONDS);
    logger.info("[BackupIntegrityMonitor] Shut down  BackupIntegrityMonitor");
  }

  private BlobStore startLocalStore(AmbryPartition partition) throws Exception {
    /**
     * Create local replica L to store cloud data
     * It will be at least as big as the largest replica of the partition on a server.
     * Cloud replica will usually be smaller than server replica, unless server disk has a problem or is compacted.
     */
    long maxReplicaSize = partition.getReplicaIds().stream()
        .map(r -> r.getCapacityInBytes())
        .max(Long::compare)
        .get();
    logger.info("[BackupIntegrityMonitor] Largest replica for partition {} is {} bytes",
        partition.getId(), maxReplicaSize);
    List<DiskId> disks = staticClusterManager.getDataNodeId(nodeId.getHostname(), nodeId.getPort())
        .getDiskIds().stream()
        .filter(d -> d.getState() == HardwareState.AVAILABLE)
        .collect(Collectors.toList());
    logger.info("[BackupIntegrityMonitor] {} disks can accommodate partition-{}", disks.size(), partition.getId());
    // Pick the next disk to make debugging easier
    DiskId disk = disks.get(++currentDiskId % disks.size());
    logger.info("[BackupIntegrityMonitor] Selected disk at mount path {}", disk.getMountPath());
    // Clear disk to make space, this is simpler instead of deciding which partition to delete.
    // This is why this is thread-unsafe.
    Arrays.stream(new File(disk.getMountPath()).listFiles()).forEach(f -> {
      try {
        Utils.deleteFileOrDirectory(f);
      } catch (Throwable e) {
        metrics.backupCheckerRuntimeError.inc();
        throw new RuntimeException(String.format("[BackupIntegrityMonitor] Failed to delete %s due to %s", f, e));
      }
    });
    // Convert Disk object to AmbryDisk object, static-map object -> helix-map object
    AmbryDisk ambryDisk = new AmbryDisk((Disk) disk, clusterMapConfig);
    AmbryServerReplica localReplica = new AmbryServerReplica(clusterMapConfig, partition, ambryDisk,
        true, maxReplicaSize, ReplicaSealStatus.NOT_SEALED);
    if (!storageManager.addBlobStore(localReplica)) {
      metrics.backupCheckerRuntimeError.inc();
      throw new RuntimeException(String.format("[BackupIntegrityMonitor] Failed to start Store for %s",
          partition.getId()));
    }
    Store store = storageManager.getStore(partition, true);
    logger.info("[BackupIntegrityMonitor] Started Store {}", store.toString());
    return (BlobStore) store;
  }

  private boolean stopLocalStore(AmbryPartition partition) {
    Store store = storageManager.getStore(partition, true);
    try {
      // Don't remove the partition from disk, leave it behind for debugging
      boolean ret = storageManager.shutdownBlobStore(partition);
      logger.info("[BackupIntegrityMonitor] Stopped Store [{}]", store);
      return ret;
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error(String.format("[BackupIntegrityMonitor] Failed to stop Store [%s]", store));
      return false;
    }
  }

  /**
   * Compares metadata received from servers with metadata received from Azure cloud
   * @param serverReplica
   * @param cloudReplica
   */
  void compareMetadata(RemoteReplicaInfo serverReplica, RemoteReplicaInfo cloudReplica) {
    long partitionId = serverReplica.getReplicaId().getPartitionId().getId();
    try {
      serverScanner.addRemoteReplicaInfo(serverReplica);
      logger.info("[BackupIntegrityMonitor] Starting scanning peer server replica [{}]", serverReplica);
      StoreFindToken newDiskToken = new StoreFindToken(), oldDiskToken = null;
      /**
       * This loop iterates through the remote peer server replica to retrieve metadata for all blobs from the
       * beginning to the current time. The termination condition relies on the token from the peer, which indicates
       * the progress of the scan. The loop stops if this token remains unchanged between successive calls to the
       * replicate() function. The essential requirement is that our scanning process must outpace the growth rate of
       * the replica. Metadata scans, being lighter than full data scans, ensure faster iteration. To ensure timely
       * termination, we must increase replicationFetchSizeInBytes significantly to 128MB or 256MB, from its
       * default of 4MB. This adjustment allows us to advance through the log faster.
       * TODO: Consider implementing a metric to monitor and prevent infinite loop.
       */
      while (!newDiskToken.equals(oldDiskToken)) {
        serverScanner.replicate();
        oldDiskToken = newDiskToken;
        newDiskToken = (StoreFindToken) serverReplica.getToken();
        logger.info("[BackupIntegrityMonitor] Scanned {} num_server_blobs from peer server replica {}",
            serverScanner.getNumBlobScanned(), serverReplica);
      }
      logger.info("[BackupIntegrityMonitor] Completed scanning {} num_server_blobs from peer server replica {}",
          serverScanner.getNumBlobScanned(), serverReplica);

      serverScanner.printKeysAbsentInServer(serverReplica);
      ReplicationMetrics rmetrics = new ReplicationMetrics(cluster.getMetricRegistry(), Collections.emptyList());
      logger.info("[BackupIntegrityMonitor] Verified cloud backup partition-{} against peer server replica {}"
              + ", num_azure_blobs = {}, num_server_blobs = {}, num_mismatches = {}",
          partitionId, serverReplica, ((RecoveryToken) cloudReplica.getToken()).getNumBlobs(),
          serverScanner.getNumBlobScanned(),
          rmetrics.backupIntegrityError.getCount());
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error(String.format("[BackupIntegrityMonitor] Failed to verify server replica %s due to",
          serverReplica), e);
      // Swallow all exceptions and print a trace for inspection, but do not kill the job
    }

    try {
      if (serverReplica != null) {
        serverScanner.removeRemoteReplicaInfo(serverReplica);
      }
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error(String.format("[BackupIntegrityMonitor] Failed to dequeue server replica %s due to",
          serverReplica), e);
      // Swallow all exceptions and print a trace for inspection, but do not kill the job
    }
  }

  /**
   * Returns CRC of blob-content
   * @param msg Metadata of blob
   * @param store Local blob store
   * @return CRC of blob-content
   */
  Long getBlobContentCRC(MessageInfo msg, BlobStore store) {
    try {
      return store.getBlobContentCRC(msg);
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      String err = String.format("[BackupIntegrityMonitor] Failed to get CRC for blob %s due to",
          msg.getStoreKey().getID());
      logger.error(err, e);
    }
    return null;
  }


  /**
   * Create a temporary map of all keys recovered from cloud
   * @param store
   * @return
   * @throws StoreException
   */
  HashMap<String, MessageInfo> getAzureBlobInfoFromLocalStore(BlobStore store) throws StoreException {
    HashMap<String, MessageInfo> azureBlobs = new HashMap<>();
    StoreFindToken newDiskToken = new StoreFindToken(), oldDiskToken = null;
    while (!newDiskToken.equals(oldDiskToken)) {
      FindInfo finfo = store.findEntriesSince(newDiskToken, 1000 * (2 << 20),
          null, null);
      for (MessageInfo msg: finfo.getMessageEntries()) {
        azureBlobs.put(msg.getStoreKey().getID(), new MessageInfo(msg, getBlobContentCRC(msg, store)));
      }
      oldDiskToken = newDiskToken;
      newDiskToken = (StoreFindToken) finfo.getFindToken();
      logger.info("[BackupIntegrityMonitor] Disk-token = {}", newDiskToken.toString());
    }
    return azureBlobs;
  }


  /**
   * This method randomly picks a partition in a cluster and locates its replica in the cluster and the cloud.
   * Then it downloads and compares data from both replicas.
   */
  @Override
  public void run() {
    RemoteReplicaInfo cloudReplica = null;
    AmbryPartition partition = null;
    serverScanner = serverReplicationManager.getBackupCheckerThread("ambry_backup_integrity_monitor");
    PartitionIdComparator partitionIdComparator = new PartitionIdComparator();
    try {
      /** Select partition P */
      List<PartitionId> partitions = helixClusterManager.getAllPartitionIds(null);
      PartitionId maxPartitionId = partitions.stream().max(partitionIdComparator).get();
      logger.info("[BackupIntegrityMonitor] Total number of partitions = {}, max partition-id = {}",
          partitions.size(), maxPartitionId.getId());
      partition = (AmbryPartition) partitions.stream()
          .filter(p -> p.getId() == currentPartitionId)
          .collect(Collectors.toList())
          .get(0);
      if (currentPartitionId == 0) {
        // We circled back to partition 0 because we either finished all partitions,
        // or there was some error in finding out the last partition scanned. Clear state.
        logger.info("[BackupIntegrityMonitor] Deleting directory {}", replicationConfig.backupCheckerReportDir);
        Utils.deleteFileOrDirectory(new File(replicationConfig.backupCheckerReportDir));
        logger.info("[BackupIntegrityMonitor] Creating directory {}", replicationConfig.backupCheckerReportDir);
        Files.createDirectories(Paths.get(replicationConfig.backupCheckerReportDir));
      }
      logger.info("[BackupIntegrityMonitor] Verifying backup partition-{}", partition.getId());

      /** Create local Store S */
      BlobStore store = startLocalStore(partition);

      /** Restore cloud backup C */
      logger.info("[BackupIntegrityMonitor] Restoring backup partition-{} to disk [{}]", partition.getId(), store);
      cloudReplica = azureReplicationManager.getCloudReplica(store.getReplicaId());
      // No need to reload tokens, since we clear off disks before each run
      azureReplicator.addRemoteReplicaInfo(cloudReplica);
      RecoveryToken azureToken = (RecoveryToken) cloudReplica.getToken();
      while (!azureToken.isEndOfPartition()) {
        azureReplicator.replicate();
        azureToken = (RecoveryToken) cloudReplica.getToken();
        long numBlobs = azureToken.getNumBlobs();
        if (numBlobs > 0 && (numBlobs % azureConfig.azureBlobStorageMaxResultsPerPage == 0)) {
          // Print progress, if N blobs have been restored from Azure
          logger.info("[BackupIntegrityMonitor] Recovered {} num_azure_blobs {} bytes of partition-{} from Azure Storage",
              numBlobs, azureToken.getBytesRead(), partition.getId());
        }
      }
      logger.info("[BackupIntegrityMonitor] Recovered {} num_azure_blobs {} bytes of partition-{} from Azure Storage",
          azureToken.getNumBlobs(), azureToken.getBytesRead(), partition.getId());

      /** Replicate from server and compare */
      // If we filter for SEALED replicas, then we may return empty as there may be no sealed replicas
      List<AmbryReplica> replicas = partition.getReplicaIds().stream()
          .filter(r -> r.getDataNodeId().getDatacenterName().equals(clusterMapConfig.clustermapVcrDatacenterName))
          .filter(r -> !r.isDown())
          .collect(Collectors.toList());
      if (replicas.isEmpty()) {
        throw new RuntimeException(String.format("[BackupIntegrityMonitor] No server replicas available for partition-%s",
            partition.getId()));
      }

      /** Compare metadata from server replicas with metadata from Azure */
      List<RemoteReplicaInfo> serverReplicas =
          serverReplicationManager.createRemoteReplicaInfos(replicas, store.getReplicaId());
      for (RemoteReplicaInfo serverReplica : serverReplicas) {
        /**
         * Find out how far each replica of the partition has been backed-up in Azure.
         * We will use this information to scan the replica until when it has been backed-up.
         * And then compare data in the replica until that point with the backup in Azure.
         */
        HashMap<String, MessageInfo> azureBlobs = getAzureBlobInfoFromLocalStore(store);
        if (azureToken.getNumBlobs() != azureBlobs.size()) {
          metrics.backupCheckerRuntimeError.inc();
          logger.warn("[BackupIntegrityMonitor] Mismatch, num_azure_blobs = {}, num_azure_blobs on-disk = {}",
              azureToken.getNumBlobs(), azureBlobs.size());
        }
        serverScanner.setAzureBlobInfo(azureBlobs);
        compareMetadata(serverReplica, cloudReplica);
      }

      currentPartitionId = ++currentPartitionId % maxPartitionId.getId();
      // Don't delete any state, leave it for inspection.
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error(String.format("[BackupIntegrityMonitor] Failed to verify cloud backup partition-%s due to",
          partition.getId()), e);
      // Swallow all exceptions and print a trace for inspection, but do not kill the job
    }

    try {
      if (cloudReplica != null) {
        azureReplicator.removeRemoteReplicaInfo(cloudReplica);
      }
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error("[BackupIntegrityMonitor] Failed to stop due to", e);
    }

    try {
      if (partition != null) {
        stopLocalStore(partition);
      }
    } catch (Throwable e) {
      metrics.backupCheckerRuntimeError.inc();
      logger.error("[BackupIntegrityMonitor] Failed to stop due to", e);
    }
  }
}
