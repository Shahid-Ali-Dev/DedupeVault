# DedupeVault – A Content-Addressable Deduplicating Backup Tool

**DedupeVault** is a dependency-free Python 3 tool for creating **deduplicated backups** using a content-addressable chunk store.  
It allows you to create snapshots of directories, restore them later, verify data integrity, and prune unused chunks.  
This is a lightweight, open-source alternative to modern backup tools like *restic* or *borg*, built entirely on Python’s standard library.

⚠️ **Disclaimer**: This project was generated with the assistance of AI and has **not been extensively tested**.  
There may be bugs or edge cases not covered. Use at your own risk and always keep a secondary backup.

---

## ✨ Features
- **Deduplication**: Identical file chunks are stored only once, saving space.
- **Snapshots**: Keep multiple backup states of your data.
- **Integrity Verification**: Verify all files and chunks via SHA-256.
- **Restore**: Recover any snapshot to a target folder.
- **Garbage Collection (Prune)**: Clean up unused chunks.
- **Cross-platform**: Works on Linux, macOS, and Windows.
- **Zero dependencies**: Uses only the Python standard library.

---

## 🚀 Getting Started

### Requirements
- **Python 3.10+**
- Works on **Windows, macOS, Linux**
  
### 📦 Usage
- Initialize a Repository
- python dedupe_backup.py init /path/to/repo

- Create a Snapshot (Backup)
- python dedupe_backup.py backup /path/to/repo /folder/to/backup

- List Snapshots
- python dedupe_backup.py list /path/to/repo
- 
- List files inside a snapshot:
- python dedupe_backup.py list /path/to/repo --snapshot 1 --files

- Restore a Snapshot
- python dedupe_backup.py restore /path/to/repo 1 /restore/target

- Verify Integrity
- python dedupe_backup.py verify /path/to/repo

- Prune Unused Chunks
- python dedupe_backup.py prune /path/to/repo

### 🛠 Project Structure
- dedupevault/
- │── dedupe_backup.py   # Main backup tool
- │── README.md          # Documentation

## ⚠️ Notes

- This project has not been tested in production.

- Created as an educational project with AI assistance.

- Expect bugs, missing features, and performance limitations.

- Contributions and testing feedback are welcome!

## 📜 License

- This project is licensed under the MIT License.
- You are free to use, modify, and distribute it.

## 🤝 Contributing

- Pull requests are welcome! If you find bugs or want to add improvements, feel free to fork the repo and submit changes.
  
### Installation
Clone the repo:
```bash
git clone https://github.com/<your-username>/dedupevault.git
cd dedupevault
