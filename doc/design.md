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
