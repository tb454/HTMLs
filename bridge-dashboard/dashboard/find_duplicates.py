import os
import hashlib

def hash_file(filepath, blocksize=65536):
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(blocksize), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def find_duplicates(root_dir):
    files_by_hash = {}
    for dirpath, _, files in os.walk(root_dir):
        for file in files:
            filepath = os.path.join(dirpath, file)
            try:
                filehash = hash_file(filepath)
            except Exception as e:
                print(f"Could not hash {filepath}: {e}")
                continue
            files_by_hash.setdefault(filehash, []).append(filepath)
    duplicates = {hash_val: paths for hash_val, paths in files_by_hash.items() if len(paths) > 1}
    return duplicates

if __name__ == '__main__':
    import sys
    root = sys.argv[1] if len(sys.argv) > 1 else '.'
    dups = find_duplicates(root)
    if dups:
        for filehash, paths in dups.items():
            print(f"Duplicate files for hash {filehash}:")
            for path in paths:
                print("  " + path)
    else:
        print("No duplicate files found.")
