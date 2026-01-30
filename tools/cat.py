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
TYPE_DATA, TYPE_TREE, TYPE_PADDING = 1, 2, 255

class MapacheRepo:
    def __init__(self, repo_path, keyfile_path, password):
        self.repo_path = repo_path
        self.path_cache = {}
        self.index_cache = {}
        self.dctx = zstd.ZstdDecompressor()

        self.dek = self._unwrap_key(keyfile_path, password)
        self.cipher = AESGCMSIV(self.dek)

    def _unwrap_key(self, keyfile_path, password):
        with open(keyfile_path, 'rb') as f:
            raw_compressed = f.read()

        try:
            # Using unlimited output size to handle missing content size headers
            kf_data = self.dctx.decompress(raw_compressed, max_output_size=1048576)
            kf = json.loads(kf_data)
        except Exception as e:
            raise ValueError(f"Failed to decompress/parse keyfile: {e}")

        kek = low_level.hash_secret_raw(
            secret=password.encode(),
            salt=base64.b64decode(kf['salt']),
            time_cost=kf['t'],
            memory_cost=kf['m'],
            parallelism=kf['p'],
            hash_len=32,
            type=low_level.Type.ID
        )

        enc_key = base64.b64decode(kf['encrypted_key'])
        # AES-GCM-SIV: [Nonce 12b][Ciphertext][Tag 16b]
        return AESGCMSIV(kek).decrypt(enc_key[:12], enc_key[12:], None)

    def _get_path(self, obj_type, obj_id):
        key = (obj_type, obj_id)
        if key in self.path_cache: return self.path_cache[key]

        if obj_type == "manifest":
            res = os.path.join(self.repo_path, "manifest")
        elif obj_type == "pack":
            res = os.path.join(self.repo_path, "objects", obj_id[:2], obj_id)
        else:
            sub = {"snapshot": "snapshots", "index": "index", "key": "keys"}.get(obj_type, obj_type + "s")
            res = os.path.join(self.repo_path, sub, obj_id)

        self.path_cache[key] = res
        return res

    def load_object(self, obj_type, obj_id, offset=0, length=None):
        path = self._get_path(obj_type, obj_id)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Object not found: {path}")

        with open(path, 'rb') as f:
            if offset: f.seek(offset)
            data = f.read(length) if length else f.read()

        if obj_type == "key":
            return self.dctx.decompress(data, max_output_size=1048576)

        # Order: Decrypt then Decompress
        decrypted = self.cipher.decrypt(data[:12], data[12:], None)
        return self.dctx.decompress(decrypted, max_output_size=10485760)

    def load_all_indices(self):
        idx_dir = self._get_path("index", "")
        if not os.path.exists(idx_dir): return
        for idx_id in os.listdir(idx_dir):
            try:
                data = json.loads(self.load_object("index", idx_id))
                for pack in data.get("packs", []):
                    pid = pack["id"]
                    for b in pack.get("blobs", []):
                        self.index_cache[b["id"]] = (pid, b["offset"], b["length"])
            except: continue

    def find_blob(self, blob_id):
        if not self.index_cache: self.load_all_indices()
        loc = self.index_cache.get(blob_id)
        if not loc: raise KeyError(f"Blob {blob_id} not in index")
        return self.load_object("pack", loc[0], loc[1], loc[2])

    def list_footer(self, pack_id):
        path = self._get_path("pack", pack_id)
        with open(path, 'rb') as f:
            f.seek(-4, os.SEEK_END)
            flen = struct.unpack('<I', f.read(4))[0]
            f.seek(-(4 + flen), os.SEEK_END)
            raw = f.read(flen)

        footer_plain = self.dctx.decompress(self.cipher.decrypt(raw[:12], raw[12:], None), max_output_size=5242880)
        res, off = [], 0
        for i in range(len(footer_plain) // FOOTER_BLOB_LEN):
            e = footer_plain[i*FOOTER_BLOB_LEN : (i+1)*FOOTER_BLOB_LEN]
            blen = struct.unpack('<I', e[33:37])[0]
            if e[32] != TYPE_PADDING:
                res.append({
                    "id": e[:32].hex(),
                    "type": "Data" if e[32] == TYPE_DATA else "Tree",
                    "offset": off, "length": blen,
                    "raw_length": struct.unpack('<I', e[37:41])[0]
                })
            off += blen
        return res

def parse_target(s):
    if s == "manifest": return "manifest", "manifest"
    return s.split(":", 1) if ":" in s else (None, s)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--keyfile", required=True)
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--footer", action="store_true")
    parser.add_argument("target", type=parse_target)
    args = parser.parse_args()

    pw = getpass.getpass("Password: ")
    try:
        repo = MapacheRepo(args.repo, args.keyfile, pw)
        t_type, t_id = args.target

        if args.footer and t_type == "pack":
            print(json.dumps(repo.list_footer(t_id), indent=4))
        elif t_type in ["blob", "tree"]:
            out = repo.find_blob(t_id)
            print(json.dumps(json.loads(out), indent=4) if args.pretty or t_type == "tree" else out.decode(errors='replace'))
        else:
            out = repo.load_object(t_type, t_id)
            if args.pretty or t_type in ["manifest", "index", "snapshot"]:
                print(json.dumps(json.loads(out), indent=4))
            else:
                sys.stdout.buffer.write(out)
    except Exception as e:
        print(f"[-] Error: {e}", file=sys.stderr)
