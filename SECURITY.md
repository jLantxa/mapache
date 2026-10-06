# Security Policy

## Reporting a Vulnerability

Do **not** report security issues in public issues or discussions.

Use the **"Report a vulnerability"** button on the repository's
[Security](https://github.com/jLantxa/mapache/security) tab — the report
stays private until it has been triaged.

Please include:

- The mapache version (`mapache --version`) and how it was installed
  (pre-built binary, `cargo install`, or build from source).
- Your operating system and repository backend (local, SFTP, S3).
- A description of the vulnerability, its impact, and concrete steps to
  reproduce.
- The assumed threat model (for example, an attacker with read access to
  a repository, or a compromised backup server).

## Supported Versions

Only the latest release receives security fixes:

| Version | Supported |
|---------|-----------|
| latest | ✅ |
| older | ❌ |

If you are running an older version, upgrade before reporting. Repository
format v1 is deprecated — migrate existing repositories to v2 with
`mapache migrate` (see the [manual](https://jlantxa.github.io/mapache/manual.html)).

## What's In Scope

mapache stores encrypted, deduplicated backups, so the following are of
particular interest:

- The repository and bundle format: confidentiality of data at rest
  (AES-256-GCM-SIV, Argon2id key derivation) and integrity (BLAKE3 content
  addressing, Reed-Solomon ECC sidecars).
- Path traversal, symlink, and special-file handling during snapshot,
  restore, import, or export.
- Remote backend handling (SFTP, S3): credential handling, path injection,
  or disclosure of plaintext or keys.
- Keyfile and passphrase handling — for example, keys being left in memory,
  swapped, or written to disk insecurely.

For the intended threat model, see the
[Security & Key Management](https://jlantxa.github.io/mapache/manual.html#13-security--key-management)
chapter of the manual.

## Response

Reports are handled on a best-effort basis. You can expect an
acknowledgement once the report has been triaged and, for valid issues,
coordinated disclosure once a fix is released. Reporters are credited in
the release notes unless anonymity is requested.