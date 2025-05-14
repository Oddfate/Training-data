{"_meta": "QuincyAmulets export", "last_updated": "2025-05-14T22:10:00-05:00"}
import chromadb
from chromadb.config import Settings
import json

# Set up Chroma client
CHROMA_PATH = "E:/QuincyWebAccessMats/Bookshelf/.chroma"
OUTPUT_FILE = "E:/QuincyWebAccessMats/.jsonl"

client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = client.get_or_create_collection(name="quincy_vector_store")

# Fetch all stored entries
results = collection.get(include=["documents", "metadatas"])

# Write to JSONL
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for uid, text, meta in zip(results["ids"], results["documents"], results["metadatas"]):
        record = {
            "uuid": uid,
            "text": text,
            "metadata": meta
        }
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

print(f"✅ Exported {len(results['ids'])} vector entries to {OUTPUT_FILE}")
