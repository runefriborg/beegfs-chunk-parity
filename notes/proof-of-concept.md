# Server design considerations

- If option A
  - Storage Node
    - 40 TB raid 6 (server mounted)
    - 108 TB raid 6 (half of a JBOD)

- If option B
  - Storage Node
    - 40 TB raid 6 (server mounted)
    - 36 TB raid 6 (1/3 half of a JBOD)
    - 36 TB raid 6 (1/3 half of a JBOD)
    - 36 TB raid 6 (1/3 half of a JBOD)
  
When a client chooses storage targets, it must never choose targets located on the same server. Not only for performance, but also to ensure that we don't loose more than one chunk of each file, when a server fails.

# Software implementation options

- Option A
  - Full extraction of meta data
  - Parallel rebuild to free space

- Option B
  - Slow identification of storage targets with difficulty of detecting missing storage chunks.
    Perform parallel traversal of filesystems of all storage targets
  - Full rebuild of a storage target to the original storaget target ID with no updates to the FhgFS meta server
  - Could do a rebuild while the filesystem is online

- Option C
  - Update parity data on every file close continusly
  - Parallel rebuild to free space
  
# Must be solved

- Option A
  - Reading meta data (offline storage nodes)
  - Updating meta data (offline storage nodes)

- Option B
  - No communication with FhGFS
  
- Option C
  - Reading meta data (offline storage nodes)
  - Updating meta data (offline storage nodes)
  - Reading from a FhGFS change log
