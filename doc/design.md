# Design

## Introduction

### The mapache repository

**Mapache** implements a `repository` as its central location for data storage.
The mapache repository is a collection of _content-addressable_ files and binary
objects distributed in a centralized layout in the repository directory.
_Content-addressable_ means that, with rare exceptions, all files and objects
are indexed not by a name or an arbitrary identifier, but the hash of its binary
contents. When mapache writes a snapshot file to the repository, it will first
calculate the hash of its content and that hash will be the name of the file.

This way of indexing data offers two interesting properties, provided that a
strong enough hash is used to avoid collisions:

1. Two identical objects will have the same ID, therefore if an object `O` with
content `C` and hash `H` exists, all binary streams with hash `H` are guaranteed
to have content `C`. This allows us to skip writing a second copy of the
identical data and grants us the certainty that content `C` exists already in
the repository.

2. If the contents of an object changed due to error or external manipulation,
its hash would not correspond to its contents. If the hash were also manipulated
to match the content, this new ID would no longer represent the original data.
This allows us to detect errors or external manipulation of the data.

In the context of mapache, an `object` is a stream of binary data represented by
its hash, or `ID`, regardless of how it is stored. An object can be stored as a
single file, with its filename being the ID of the object, or bundled with other
objects in a file with any known arbitrary format. In any case, the filename
will identify its contents as a whole by their hash.

The mapache repository is a tree directory that contains files with different
purposes:

```text
repo
├── index
│   └── 771f7c9d05241c55a87e004b148310953c1565ef10f55daf02a68ba0ea4e0166
├── keys
│   └── cea3212843e488b168f6bb21b757d6cb4882a559e10ec4b33f305e982c4c3c8e
├── locks
├── manifest
├── objects
│   ├── 00
│   ├── 01
│   ├── ..
│   ├── ff
│   │   └── ff890e7aeb656e7f57004e05009fa66576448a150c6471d38e11e4e6d0f3227b
└── snapshots
    └── 1d0e210a91f77744f87420387e52a108b87fd900a1d8462f3b9def4b99a47b9e
    └── 84afca27f7c134131cf8d33a2ae70114889c3c31dc281b7fffad998e7ddb569a.dropped
```

### Storage

Mapache is a de-duplicating, incremental backup program. It implements a
content-defined chunking algorithm to split files into small chunks. Every chunk
is identified by the hash of its raw data, or ID. This algorithm is able do
detect content boundaries, so when you modify the file, the algorithm can detect
the new data added as a series of new chunks.
All these file chunks, or `data blobs` are stored in the `objects` directory,
which works as a `blob` library. Since we can identify every blob by its ID, a
single blob is stored only once. If two files share a blob, this blob is saved
only once in the repository. When a file is saved along multiple snapshots,
mapache will chunk the file and only store the new blobs that do not exist
already in that blob library.

A **snapshot** is akin to a still photograph of the file system at a point of
time. The content of files will be deduplicated and encoded into blobs in the
object storage. However, if we only stored data blobs, we would not be able to
reconstruct the original file system tree, the metadata, permissions, file times
and attributes.
Mapache stores the tree metadata as blobs in the object storage. File nodes
contain the list of data blobs that constitute its contents, symlinks contain
the target path, directory nodes have a reference to its own subtree, etc. These
medatada blobs are also deduplicated so that if two trees are exactly
identical, there only exists one copy in the object storage. This deduplication
reduces the amount of data necessary to store the snapshot metadata, since most
of the time a snapshot only introduces a few modified or new trees.
The snapshot file will reference the root tree, and contain metadata like a
timestamp, host, tags, description, etc.

Blobs in the object storage are not stored as individual files. We could do
that, but it would be highly inefficient. A generic repository could potentially
contain millions of blobs, exhausting the available file system inodes and
requiring too many I/O operations to store all the blobs. Instead, blobs are
packed in `pack files`.
A pack file contains multiple blobs and some metadata describing them. Pack
files can be configured to have any size up to 4 GiB. In order to find blobs
efficiently within the pack files, the mapache repository maintains an index.
This index, composed of one or more index files, describes the packs and their
contents and allows mapache to find a blob by mapping the blob ID to the pack
ID, its offset, length and type.

The **index** is the central piece of how mapache works. If a blob ID is not
referenced by the index, the blob does not exist for mapache and will be subject
to elimination by the garbage collector, even if there is a pack file that
contains it. This allows mapache to append new data atomically. If a backup is
interrupted, all blobs and packs not referenced by a persisted index will be
left dangling, as if they were never added. Restarting the backup will not
resume the process from where it was interrupted, but all indexed blobs will
not be rewritten. To avoid losing all progress, mapache may periodically
persist all finalized indices to file.

### Chunking

Mapache uses a custom implementation of [FastCDC](https://ieeexplore.ieee.org/document/9055082).
FastCDC is a Content-Defined Chunking algorithm proposal that offers a speedup
of 3x - 12x with respect to Rabin-based CDC with comparable deduplication ratio.

### Compression and encryption

A good chunker can do wonders to reduce the amount of data needed to store all
the files of a snapshot, but we can go one step further. Mapache uses `zstd` to
compress **almost** all the data stored in the repository, including data and
tree blobs. The compression level can be configured, affecting the compression
ratio and the total time needed to take a snapshot.

Security is a non-negotiable aspect of mapache by design. **Everything** except
for a handful of bytes is encrypted. Encryption cannot be disabled or opted-out.
Mapache implements AES-GCM-SIV. This is a modern cipher which implements
encryption and authentication of data with a key. AES-GCM-SIV not only encrypts
the data, it adds an authentication layer that allows mapache to detect when an
object has been altered or manipulated.

#### Master key and key files

All the encrypted content in the mapache repository is encrypted with a unique
256-bit master key. This key is randomly generated when the repository is
created. Users do not access the repository with the master key directly.
Instead, they are asked for a username and a password. The password is used to
derive a 256-bit personal key using Argon2. The personal key is solely used to
encrypt personal copies of the master key in a KeyFile. Every user has a KeyFile
that they use to open the repository. These KeyFiles can be stored in the `keys`
directory or provided externally.

Mapache does not implement master key rotation at the moment, but it may do so
in the future.

### Hashing

All content IDs (hashes) used by mapache to identify objects in the repository
are generated with the BLAKE3 hashing algorithm. BLAKE3 is a modern and fast
hashing algorithm that produces 256-bit hashes.

## Threat model

Mapache assumes that the host system is secure. However, the repository itself
may reside on an untrusted medium where an attacker has the ability to read,
modify, or delete the repository files.

### Security Guarantees

Mapache provides strong guarantees for the security of the data, but it cannot
prevent file system operations by an external attacker:

- **Confidentiality**: An attacker should not be able to decipher the stored
  data.
- **Integrity (Verifiability)**: Mapache can detect if the data has been
  modified or corrupted.

### Protection Mechanisms

Mapache uses a dual-layer approach to ensure data integrity and confidentiality:

- **Authenticated Encryption with Associated Data (AEAD):** All data is
  protected using an AEAD cipher (AES-GCM-SIV). This means the information is
  both encrypted (ensuring confidentiality) and authenticated. The
  authentication layer allows Mapache to detect if a file has been altered or
  manipulated externally.
- **Content-Addressability:** Objects are identified by the cryptographic hash
  (ID) of their content (BLAKE3). This provides a secondary and independent
  verification mechanism to ensure the content has not been corrupted.

### Attacker Limitations

Due to the mandatory encryption and authentication:

- An attacker who reads the data will only obtain unintelligible, encrypted
  information.
- An attacker who modifies or renames files will cause the corresponding
  integrity check (both AEAD and hash verification) to fail, rendering the data
  unusable by Mapache.

Therefore, the most effective action an attacker on the untrusted medium can
take is the destruction (deletion) of the repository files. Mapache is unable to
prevent files in the repository from being modified, renamed, or deleted by an
external entity with access to the storage medium.

## Repository format

The mapache repository stores metadata in JSON format.

### Manifest

The `manifest` file describes the repository: its ID, creation time and version
number. The ID is a 256-bit identifier generated randomly at the time of
creation.

The `manifest` file is compressed and encrypted with the master key.

```json
{
  "version": 1,
  "id": "b7468f6331331302b06c63b98a14e50107f9cc26683afd064c0af84eec53b3e7",
  "created_time": "2025-11-20T13:06:52.266751300+01:00"
}
```

### Snapshots

The `snapshot` file contains metadata about a snapshot: timestamp, user name,
host name,paths, root tree, summary, etc.
The file name (ID) is the hash of the encoded content.

```json
{
  "timestamp": "2025-11-20T19:00:00.375479500+01:00",
  "tree": "b266241241162f84e91165d96d03de0719b385c5b2918cec02b5324869e6145c",
  "root": "I:\\mapache",
  "paths": [
    "I:\\mapache\\core"
  ],
  "hostname": "cocoon",
  "username": "mapache",
  "summary": {
    "processed_items_count": 94,
    "processed_bytes": 744155,
    "raw_bytes": 744155,
    "encoded_bytes": 197650,
    "meta_raw_bytes": 42694,
    "meta_encoded_bytes": 18639,
    "total_raw_bytes": 786849,
    "total_encoded_bytes": 216289,
    "new_files": 79,
    "deleted_files": 0,
    "changed_files": 0,
    "new_dirs": 15,
    "deleted_dirs": 0,
    "changed_dirs": 0,
    "unchanged_files": 0,
    "unchanged_dirs": 0
  }
}
```

### Packs and blobs

Mapache stores backup data in the objects directory. A fanout of 1 byte is used
to reduce the number of files per directory: 256 directories with names 00 to ff
contain the pack files. A packfile with ID `dd2856e...` will be stored in `objects/dd/dd2856e9f7ac9b495e7ebce1370f68a2e60824cee9546b817f7cbbb4d571bd4b`.

All blobs are stored in pack files, which group multiple blobs together to
optimize storage and I/O operations. The pack file name (ID) is the hash of its
content. Since a pack file contains already pre-encoded blobs and an encoded
header, no further encoding is applied.

The layout of a pack file is the following.

Encoded pack 1
Encoded pack 2
...
Encoded pack N
PACK HEADER

The blobs are encoded and appended to the pack. Their ID is the hash of their
raw content. The pack header describes the blobs contained in the pack header.
This header includes a 4-byte field which is the length of the total header. The
pack header is encoded, except for the length field.

#### Trees

Mapache stores file contents (data blobs) as well as tree metadata (tree blobs).
Tree and data blobs are stored in separate packs. The data blobs contain chunks
of the file contents. The tree blob contain metadata about the file system tree
nodes in JSON format. Data and tree blobs are stored in pack files in the same
way. Each tree is stored as a single blob. A tree node can be a file, directory,
symlinkm etc. Directories have a `tree` field which points to its subtree. File
nodes have a `blobs` field with contains a list of its data blobs in order.
Symlinks have a field `symlink_info` which contains metadata about the target
path and node type of the target.

```json
{
  "name": "tests",
  "type": "directory",
  "metadata": {
    "size": 0,
    "created_time": {
      "secs_since_epoch": 1763560510,
      "nanos_since_epoch": 935069400
    },
    "modified_time": {
      "secs_since_epoch": 1763560510,
      "nanos_since_epoch": 937075900
    }
  },
  "tree": "093ca4db9e88163d1e9e815e01d9b3a6e7e7980d21a674e433eb75fdb2f133cd"
}
```

```json
{
  "nodes": [
    {
      "name": "integration_tests",
      "type": "directory",
      "metadata": {
        "size": 4096,
        "created_time": {
          "secs_since_epoch": 1763560510,
          "nanos_since_epoch": 935069400
        },
        "modified_time": {
          "secs_since_epoch": 1763560510,
          "nanos_since_epoch": 937075900
        }
      },
      "tree": "9a426139fa0315872bb3da4ee6ed6f0955f7425846efd0c22c3d040d7f9dc9d7"
    },
    {
      "name": "tests.rs",
      "type": "file",
      "metadata": {
        "size": 834,
        "created_time": {
          "secs_since_epoch": 1763560510,
          "nanos_since_epoch": 937075900
        },
        "modified_time": {
          "secs_since_epoch": 1763560510,
          "nanos_since_epoch": 937075900
        }
      },
      "blobs": [
        "6772c0355a4f84a095343951e371677fc88ba92820bc4e12ab78c9de61ae3ea2"
      ]
    }
  ]
}
```

#### Pack header format

The pack header consists of a variable-length list of metadata blob entries, each 41 bytes
long, followed by a fixed-size trailer. Each entry contains an ID (32 bytes), the
blob type (u8), and both the encoded length and raw length (u32) of the associated
data blob. The trailer is a single u32 field which stores the total length of the
entire pack header, allowing a parser to efficiently skip directly to the file's data section.
All data except the header length field is encrypted.

```text
┌────────┬────────┬─────┬────────┬─────────────────────┐
│ Blob 1 │ Blob 2 │ ... │ Blob N │ Header length (u32) │
└────────┴────────┴─────┴────────┴─────────────────────┘

┌──────────────────────────────────────────┐
│     Pack header blob entry (41 bytes)    │
│────────────────────────┬────────┬────────│
│ Field                  │  Size  │ Offset │
│────────────────────────┼────────┼────────│
│ ID (256-bit raw hash)  │   32   │   0    │ Hash of the raw blob data
│────────────────────────┼────────┼────────│
│ Blob type (u8)         │    1   │   32   │
│────────────────────────┼────────┼────────│
│ Encoded length (u32)   │    4   │   33   │ Length of the encoded blob
│────────────────────────┼────────┼────────│
│ Raw length (u32)       │    4   │   37   │
└────────────────────────┴────────┴────────┘
  ^ (41 bytes)
```

### Index

### Locks

Lock files are the locking and synchronization mechanism at the repository
level. When mapache opens a repository, it may need to acquire an exclusive or
non-exclusive lock. Non-exclusive locks allow concurrent access with other
non-exclusive locks. They are used for constructive operations which do not
affect other users. Exclusive locks are used to get single, exclusive access to
the repository for destructive operations like forget or clean.

A lock is considered expired if its timestamp is older than a predetermined
threshold. Expired locks are not considered when trying to acquire a new lock
and get deleted by the garbage collection mechanism. During normal operation,
locks get periodically refreshed. If the process that holds a lock terminates
abruptly, it might not release its lock. The expiration time serves as a way
to remove those locks which did not get deleted.

The lock name is a unique 256-bit ID generated randomly at the time of creation
and has nothing to do with its content. A lock that is refreshed will conserve
its ID, even though the timestamp will change.

```json
{
  "id": "3468acbec193d65560c8ff16fea30e2c566d48ddf1234aefa9b1bac09c1cf71f",
  "timestamp": "2025-11-20:00:00.390485800+01:00",
  "exclusive": false,
  "hostname": "cocoon",
  "username": "mapache",
  "pid": 4216
}
```

### Keys

KeyFiles serve as keys to the repository. They contain metadata about the user,
creation time, parameters for the password derivation function and the encrypted
master key, using the password and a salt. KeyFiles can be stored in the `keys`
directory or outside of the repository. KeyFiles are compressed but not
encrypted. The KeyFile name (ID) is the hash of the encoded (compressed)
content.

```json
{
  "created": "2025-11-20T19:00:00.663953700+01:00",
  "username": "mapache",
  "m": 19456,
  "t": 2,
  "p": 1,
  "salt": "VRKrdGuf5T8iW3b5VYgJ+bMTMrYKRphbxkq2qW6C7VQ=",
  "encrypted_key": "bkwxbjuLbV094UiRFoO5t2YlV3ZhsCQdn3z2A+YyPSNewGFUo+AwL+L7Jae406yW4OHcHzFwN3wv7Vlj"
}
```

## Local cache

Mapache maintains a local cache to speedup some commands and minimize the
number of remote I/O operations. Certain metadata like tree blobs, snapshots and
indices are cached locally to avoid having to download them from a remote
backend. The cache is a mirror copy of the mapache repository layour, but it
only contains those files that are eligible for caching. These cache directories
are stored in the usual cache directory of the OS.

Mapache provides a `cache` command to list and manage cache folders, including
a cleanup option.
