package com.github.ambry.store;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// This class could have a "main" method named
// checkAndMaybeTerminate()?
public class ReplicaPlacementChecker {
  private final Map<DiskId, Set<String>> foundDiskToPartitionMap;
  private Map<DiskId, List<ReplicaId>> expectedDiskToReplicaMap;
  private Map<DiskId, List<ReplicaId>> newDiskToReplicaMap;
  private int brokenDisks = 0;
  private List<DiskId> bustedDisks = new ArrayList<>();
  public ReplicaPlacementChecker(Map<DiskId, List<ReplicaId>> expectedDiskToReplicaMap) {
    foundDiskToPartitionMap = new HashMap<>();
    newDiskToReplicaMap = new HashMap<>();
    this.expectedDiskToReplicaMap = expectedDiskToReplicaMap;

    for (Map.Entry<DiskId, List<ReplicaId>> entry : expectedDiskToReplicaMap.entrySet()) {
      DiskId currentDisk = entry.getKey();
      foundDiskToPartitionMap.put(currentDisk, findPartitionsOnDisk(currentDisk));
      List<ReplicaId> replicas = entry.getValue();
      for (ReplicaId replica : replicas) {
        String partitionID = replica.getPartitionId().toString();
        if(!foundDiskToPartitionMap.get(currentDisk).contains(partitionID)) {
          brokenDisks++;
          bustedDisks.add(currentDisk);
        }
      }
    }

    if(brokenDisks > 0 && brokenDisks % 2 == 0) {
      if(reshuffleDisks()) {
        // Write new state to Helix
        // Terminate
      }
    }
  }

  private boolean reshuffleDisks() {
    if(expectedDiskToReplicaMap.size() != foundDiskToPartitionMap.size()) {
      return false;
    }

    for (DiskId currentDisk : bustedDisks) {
      List<ReplicaId> expectedReplicas = expectedDiskToReplicaMap.get(currentDisk);
      DiskId foundDisk = findDiskWithReplicas(expectedReplicas);
      if(foundDisk == null) {
        return false;
      } else {
        // Tommy: After this, we can use newDiskToReplicaMap in conjunction with expectedDiskToReplicaMap
        //        to update Helix. If a disk is NOT present in newDiskToReplicaMap, then we just use
        //        expectedDiskToReplicaMap. Otherwise we use newDiskToReplicaMap for the Helix update.
        newDiskToReplicaMap.put(currentDisk, expectedReplicas);
      }
    }

    return true;
  }

  private DiskId findDiskWithReplicas(List<ReplicaId> expectedReplicas) {
    for (Map.Entry<DiskId, Set<String>> entry : foundDiskToPartitionMap.entrySet()) {
      DiskId currentDisk = entry.getKey();
      Set<String> partitions = entry.getValue();
      boolean found = true;
      for (ReplicaId replica : expectedReplicas) {
        String partitionID = replica.getPartitionId().toString();
        if(!partitions.contains(partitionID)) {
          found = false;
          break;
        }
      }
      if(found) {
        return currentDisk;
      }
    }
    return null;
  }

  /**
   * Checks whether a directory name looks like a partition.
   * @param directoryName the name of the directory to check.
   * @return True if the directory name looks like a partition, false otherwise.
   */
  private boolean looksLikePartition(String directoryName) {
    try {
      // The convention is simply to use Java Long as partition IDs.
      Long.parseLong(directoryName);
    } catch (NumberFormatException e) {
      return false;
    }
    return true;
  }

  /**
   * Find all the partition directories on the disk.
   * @param disk an instance of DiskId to search for partition directories.
   * @return A list of partition directories on the disk.
   */
  private Set<String> findPartitionsOnDisk(DiskId disk) {
    Set<String> partitionDirs = new HashSet<>();
    File[] directories = new File(disk.getMountPath()).listFiles(File::isDirectory);

    if (directories != null) {
      for (File dir : directories) {
        // Tommy: If we store just the leaf name from File.getName() will that work
        //        when comparing with the directory names we get from ReplicaId??
        //        AmbryPartition::toPathString() returns the partition ID as a string.
        String dirName = dir.getName();
        if (looksLikePartition(dirName)) {
          partitionDirs.add(dirName);
        }
      }
      return partitionDirs;
    }
  }

  // Foreach disk
  //     Find all the partition directories on the disk
  //     Add them to actualDiskToReplicaMap
  //     Foreach replica entry in expectedDiskToReplicaMap
  //         if(not found in actualDiskToReplicaMap)
  //             badDisks++
  //
  // if(badDisks > 0 && badDisks % 2 == 0)
  //     reshuffleDisks()
  //     if(successful)
  //         write new state to Helix
  //         terminate

  // Reshuffling Disks:
  //     First, confirm that both the expected and actual maps have the same number of
  //     keys. Otherwise, somehow not all the disks are present, and we should not proceed.
  //
  //     foreach(DiskId disk : expectedDiskToReplicaMap.keySet())
  //         List<ReplicaId> expectedReplicas = expectedDiskToReplicaMap.get(disk)
  //         DiskId foundDisk = findDiskWithReplicas(expectedReplicas)
  //         if(foundDisk == null)
  //             return false
  //         else
  //             actualDiskToReplicaMap.put(disk, expectedReplicas)
  //
  //      if(actualDiskToReplicaMap.size() == expectedDiskToReplicaMap.size())
  //          We succeeded, so we can update Helix.

}
