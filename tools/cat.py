import json
import base64
import zstandard as zstd
import sys
import argparse
import getpass
import os
import struct
from argon2 import low_level
from cryptography.hazmat.primitives.ciphers.aead import AESGCMSIV

# Mapache Constants
FOOTER_BLOB_LEN = 41

# BlobType enum values (from common/id.rs)
BLOB_TYPES = {0: "Data", 1: "Tree", 2: "Padding", 3: "Zero"}


class MapacheRepo:
    def __init__(self, repo_path, keyfile_path, password):
        self.repo_path = repo_path
        self.path_cache = {}
        self.index_cache = {}
        self.dctx = zstd.ZstdDecompressor()

        self.dek = self._unwrap_key(keyfile_path, password)
        self.cipher = AESGCMSIV(self.dek)
        self.repo_version = self._detect_version()

    def _unwrap_key(self, keyfile_path, password):
        with open(keyfile_path, 'rb') as f:
            raw_compressed = f.read()

        try:
            kf_data = self.dctx.decompress(raw_compressed, max_output_size=1048576)
            kf = json.loads(kf_data)
        except Exception as e:
            raise ValueError(f"Failed to decompress/parse keyfile: {e}")

        kdf = kf['kdf']
        time_cost = kdf['t']
        memory_cost = kdf['m']
        parallelism = kdf['p']

        kek = low_level.hash_secret_raw(
            secret=password.encode(),
            salt=base64.b64decode(kf['salt']),
            time_cost=time_cost,
            memory_cost=memory_cost,
            parallelism=parallelism,
            hash_len=32,
            type=low_level.Type.ID
        )

        enc_key = base64.b64decode(kf['encrypted_key'])
        return AESGCMSIV(kek).decrypt(enc_key[-12:], enc_key[:-12], None)

    def _detect_version(self):
        """Read and validate the repository format version."""
        path = os.path.join(self.repo_path, "manifest")
        with open(path, 'rb') as f:
            raw = f.read()
        decrypted = self._decrypt(raw)
        decompressed = self.dctx.decompress(decrypted, max_output_size=1048576)
        version = json.loads(decompressed).get("version")
        if version != 2:
            raise ValueError(f"unsupported repository format version: {version}")
        return version

    def _get_path(self, obj_type, obj_id):
        key = (obj_type, obj_id)
        if key in self.path_cache:
            return self.path_cache[key]

        if obj_type == "manifest":
            res = os.path.join(self.repo_path, "manifest")
        elif obj_type == "pack":
            res = os.path.join(self.repo_path, "objects", obj_id[:2], obj_id)
        else:
            sub = {"snapshot": "snapshots", "index": "index", "key": "keys"}.get(obj_type, obj_type + "s")
            res = os.path.join(self.repo_path, sub, obj_id)

        self.path_cache[key] = res
        return res

    def _decrypt(self, data):
        """Decrypt v2 data laid out as [ciphertext+tag | nonce(12)]."""
        nonce = data[-12:]
        ct = data[:-12]
        return self.cipher.decrypt(nonce, ct, None)

    def load_object(self, obj_type, obj_id, offset=0, length=None):
        path = self._get_path(obj_type, obj_id)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Object not found: {path}")

        with open(path, 'rb') as f:
            if offset:
                f.seek(offset)
            data = f.read(length) if length else f.read()

        if obj_type == "key":
            return self.dctx.decompress(data, max_output_size=1048576)

        decrypted = self._decrypt(data)
        return self.dctx.decompress(decrypted, max_output_size=10485760)

    def load_all_indices(self):
        """Load all binary index files."""
        idx_dir = self._get_path("index", "")
        if not os.path.exists(idx_dir):
            return
        for idx_id in os.listdir(idx_dir):
            try:
                raw = self.load_object("index", idx_id)
                packs = self._parse_index_binary(raw)

                for pack in packs:
                    pid = pack["id"] if isinstance(pack["id"], str) else pack["id"].hex()
                    for b in pack.get("blobs", []):
                        bid = b["id"] if isinstance(b["id"], str) else b["id"].hex()
                        self.index_cache[bid] = (pid, b["offset"], b["length"])
            except Exception:
                continue

    def _parse_index_binary(self, data):
        """Parse v2 binary index format."""
        if len(data) < 12 or data[:4] != b"MPIX":
            raise ValueError("invalid binary index header")
        version, flags = struct.unpack_from('<HH', data, 4)
        if version != 2 or flags != 0:
            raise ValueError("unsupported binary index version or flags")

        packs = []
        cur = data

        num_packs = struct.unpack_from('<I', cur, 8)[0]
        pos = 12

        for _ in range(num_packs):
            pack_id = cur[pos:pos + 32]
            pos += 32
            blob_count = struct.unpack_from('<I', cur, pos)[0]
            pos += 4

            blobs = []
            for _ in range(blob_count):
                blob_id = cur[pos:pos + 32]
                pos += 32
                type_byte = cur[pos]
                pos += 1
                offset = struct.unpack_from('<I', cur, pos)[0]
                pos += 4
                length = struct.unpack_from('<I', cur, pos)[0]
                pos += 4
                raw_length = struct.unpack_from('<I', cur, pos)[0]
                pos += 4

                blob_type_val = type_byte & 0x7F
                compressed = type_byte & 0x80 != 0
                blob_type_name = BLOB_TYPES.get(blob_type_val, f"Unknown({blob_type_val})")

                blobs.append({
                    "id": blob_id,
                    "type": blob_type_name,
                    "compressed": compressed,
                    "offset": offset,
                    "length": length,
                    "raw_length": raw_length,
                })

            packs.append({"id": pack_id, "blobs": blobs})

        return packs

    def find_blob(self, blob_id):
        if not self.index_cache:
            self.load_all_indices()
        loc = self.index_cache.get(blob_id)
        if not loc:
            raise KeyError(f"Blob {blob_id} not in index")
        return self.load_object("pack", loc[0], loc[1], loc[2])

    def list_footer(self, pack_id):
        path = self._get_path("pack", pack_id)
        with open(path, 'rb') as f:
            f.seek(-4, os.SEEK_END)
            flen = struct.unpack('<I', f.read(4))[0]
            f.seek(-(4 + flen), os.SEEK_END)
            raw = f.read(flen)

        footer_plain = self.dctx.decompress(
            self._decrypt(raw),
            max_output_size=5242880
        )
        if footer_plain[:4] != b"MPFT" or struct.unpack_from('<H', footer_plain, 4)[0] != 2:
            raise ValueError("unsupported pack footer format")
        if struct.unpack_from('<H', footer_plain, 6)[0] != 0:
            raise ValueError("invalid pack footer flags")
        footer_plain = footer_plain[8:]
        if len(footer_plain) % FOOTER_BLOB_LEN:
            raise ValueError("partial pack footer descriptor")
        res, off = [], 0
        for i in range(len(footer_plain) // FOOTER_BLOB_LEN):
            e = footer_plain[i * FOOTER_BLOB_LEN:(i + 1) * FOOTER_BLOB_LEN]
            blen = struct.unpack('<I', e[33:37])[0]
            type_byte = e[32]
            blob_type = type_byte & 0x7F
            if blob_type != 2:
                res.append({
                    "id": e[:32].hex(),
                    "type": BLOB_TYPES.get(blob_type, f"Unknown({blob_type})"),
                    "compressed": bool(type_byte & 0x80),
                    "offset": off, "length": blen,
                    "raw_length": struct.unpack('<I', e[37:41])[0]
                })
            off += blen
        return res


def parse_target(s):
    if s == "manifest":
        return "manifest", "manifest"
    # Normalize keyfile → key
    if s.startswith("keyfile:"):
        s = "key:" + s[len("keyfile:"):]
    return s.split(":", 1) if ":" in s else (None, s)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Inspect objects in a Mapache repository.",
        epilog="""\
target types:
  manifest              Repository manifest
  snapshot:ID           Snapshot by ID (prefix)
  pack:ID               Pack file by ID (prefix)
  index:ID              Index file by ID (prefix)
  key:ID                Key file by ID (prefix)
  keyfile:ID            Alias for key:ID
  blob:ID               Data blob by ID (looked up via index)
  tree:ID               Tree blob by ID (looked up via index)

examples:
  %(prog)s --repo /repo --keyfile kf.mapache --pretty manifest
  %(prog)s --repo /repo --keyfile kf.mapache --pretty snapshot:abc123
  %(prog)s --repo /repo --keyfile kf.mapache --footer pack:def456
  %(prog)s --repo /repo --keyfile kf.mapache blob:789abc
  %(prog)s --dump-keyfile kf.mapache
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--repo", help="Path to the Mapache repository")
    parser.add_argument("--keyfile", help="Path to the keyfile")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    parser.add_argument("--footer", action="store_true", help="Show pack footer (with --footer pack:ID)")
    parser.add_argument("--dump-keyfile", metavar="KEYFILE", help="Dump keyfile JSON (no password needed)")
    parser.add_argument("target", nargs="?", type=parse_target, help="Object to read (type:id)")
    args = parser.parse_args()

    # --dump-keyfile mode: read and print keyfile contents
    if args.dump_keyfile:
        try:
            with open(args.dump_keyfile, 'rb') as f:
                raw_compressed = f.read()
            dctx = zstd.ZstdDecompressor()
            kf_data = dctx.decompress(raw_compressed, max_output_size=1048576)
            kf = json.loads(kf_data)
            print(json.dumps(kf, indent=2))
        except Exception as e:
            print(f"[-] Error: {e}", file=sys.stderr)
        sys.exit(0)

    # Normal mode: require --repo, --keyfile, and target
    if not args.repo:
        parser.error("--repo is required (or use --dump-keyfile)")
    if not args.keyfile:
        parser.error("--keyfile is required (or use --dump-keyfile)")
    if not args.target:
        parser.error("target is required (e.g. manifest, snapshot:ID, pack:ID)")

    pw = getpass.getpass("Password: ")
    try:
        repo = MapacheRepo(args.repo, args.keyfile, pw)
        print(f"[*] Repository version: {repo.repo_version}", file=sys.stderr)
        t_type, t_id = args.target

        if args.footer and t_type == "pack":
            print(json.dumps(repo.list_footer(t_id), indent=4))
        elif t_type in ["blob", "tree"]:
            out = repo.find_blob(t_id)
            print(json.dumps(json.loads(out), indent=4) if args.pretty or t_type == "tree" else out.decode(errors='replace'))
        else:
            out = repo.load_object(t_type, t_id)
        if args.pretty or t_type in ["manifest", "index", "snapshot", "key"]:
            print(json.dumps(json.loads(out), indent=4))
        else:
            sys.stdout.buffer.write(out)
    except Exception as e:
        print(f"[-] Error: {e}", file=sys.stderr)
