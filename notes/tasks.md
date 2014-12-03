
*Authors: Rune M. Friborg and Anders Halager*

# parity-gen
- Abort unless all systems are online
- Initial read of meta data (describe requirement to Sven)
- Clean parity-db and old parity chunks.
  Remove every parity-db entry and create delete lists for every storage targets
- Make every MPI-thread on every storage target delete old parity chunks
- Create disk usage array for all storage targets
- Build tasklist containing updated files since last parity-gen.
 - task (use data from parity-db if available):
    - file-id
    - full file size
    - chunk storage target IDs (reduce the amount of storage target IDs, depending on the chunk size
      info and the full file size)
    - P storage target ID (choose based on a LRU table of storage target IDs,
      weighting and disk usage)
    - Q storage target ID (choose based on a LRU table of storage target IDs,
      weighting and disk usage)
    - Update disk usage array
- Create storage-target-ID-to-MPI-rank map
- Initialise parity-gen MPI-thread on every storage target
- Receive a notification for every task:
  - Update parity-db depending on task result
  - Construct list of erroneous tasks
  - Continuously write status to stdout/log based on settings
- If the list of erroneous tasks is above N for a single storage target, then
  we may have a storage target failure and we will not delete anything.
- Cleanup after erroneous tasks

# parity-rebuild
- Only run if mgmt+meta data servers online and all storage servers are marked as offline
- Provide target-id to rebuild, provide target-id of other failed storage target
- Initial read of meta data (describe requirement to Sven)
- Read parity-db
- For every file entry in meta data list:
  - If the storage targets of the file does not match the rebuild-target-id. Skip file.
  - If the file entry is not in the parity-db. Skip file and report error to log.
  - Add file entry to tasklist.
  - task:
    - file-id
    - full file size (from parity-db)
    - error-code (0 for OK, 1 if file size in meta data is different from parity-db, or
      2 if modify-data is newer than last-updated-parity date)
    - chunk storage target IDs (reduce the amount of storage target IDs,
      depending on the chunk size info and the full file size) <-- Replace rebuild-target-id with new storage target ID (choose based on a LRU table of storage target IDs, weighting and disk usage)
    - P storage target ID
    - Q storage target ID
- Create storage-target-ID-to-MPI-rank map
- Initialise parity-rebuild MPI-thread on every storage target
- Receive a notification for every task:
  - Update parity-db depending on task result
  - Construct list of erroneous tasks
  - Continuously write status to stdout/log based on settings
- Manually, when all storage targets have been rebuilt.
 - run fhgfs-fsck
 - run parity-fsck
 - run parity-gen


# parity-fsck
- Remove all parity chunks without an entry in parity-db
- Remove all entries in parity-db with missing parity-chunks



# parity-db content:
* file-id (eg. hash)
* last-updated-parity
* P storage target id
* Q storage target id


In parity-gen MPI-thread every storage target performs the following:
* Receives full tasklist
* Set message size to 1MByte
* For every item in tasklist:
  - If not in item, then skip to next item
  - Take one of 3 roles, depending on the storage target
    1. `chunk-sender` asynchronously send chunks in 1MByte messages to the parity-generator-rank
    2. `parity-generator`
      * Receive all chunks asynchronously
      * In parallel compute the parity for the previously received 1MByte messages
      * Asynchronously send parity to disk and to parity-receiver.
        OBS: Do not truncate previously written parity, until the first valid parity have been generated.
      * When done, inform parity-gen coordinator of the task state.
      * Possible states:
        * OK, update parity-db
        * Error, data was updated during parity-generation. Schedule at parity-gen coordinator to remove from parity-db (+ old parity chunks)
        * Error, chunk data missing. Schedule at parity-gen coordinator to remove from parity-db ( + old parity chunks)
    3. `parity-receiver`
      Receive parity and write to disk.
      OBS: Do not truncate previously written parity, until the first valid message have been received.

In parity-rebuild MPI-thread every storage target performs the following:
* Receives full tasklist
* Set message size to 1MByte
* For every item in tasklist:
  - If not in item, then skip to next item
  - Take one of 3 roles, depending on the storage target
