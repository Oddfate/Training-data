# === VEC-RAG PIPELINE — FINAL SYMBOLIC BUILD FOR 🔤 ===
# Includes:
# - Path selection on launch
# - File-type routing
# - Dupe prevention via SHA256
# - AI-first metadata catalog
# - Symbolic progress tracker
# - Outputs: vector store + index.md + catalog.jsonl

import os
import hashlib
import json
import time
import subprocess
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings
import fitz  # PyMuPDF for PDFs
import docx  # python-docx for DOCX
from ebooklib import epub  # EPUB files
from bs4 import BeautifulSoup  # HTML/EPUB parsing
from PIL import Image  # Image OCR
import pytesseract
import whisper  # Audio transcription
import tempfile  # For video audio extraction
from moviepy.editor import VideoFileClip

# 👇 Force writable temp folder for MoviePy/FFMPEG
TEMP_DIR = "E:/QuincyWebAccessMats/temp_processing"
os.makedirs(TEMP_DIR, exist_ok=True)
tempfile.tempdir = TEMP_DIR

# === CONFIG ===
OUTPUT_ROOT = r"E:\\QuincyWebAccessMats"
VEC_OUTPUT_DIR = os.path.join(OUTPUT_ROOT, "Bookshelf")
INDEX_MD = os.path.join(OUTPUT_ROOT, "index.md")
CATALOG_JSONL = os.path.join(OUTPUT_ROOT, "catalog.jsonl")
TRACKER_FILE = os.path.join(OUTPUT_ROOT, "progress_tracker.json")
HASH_LEDGER = os.path.join(OUTPUT_ROOT, "hashes.json")
CHROMA_PATH = os.path.join(VEC_OUTPUT_DIR, ".chroma")
MODEL_NAME = "all-MiniLM-L6-v2"
SEVEN_ZIP_PATH = r"C:\\Program Files\\7-Zip\\7z.exe"

# === INIT ===
os.makedirs(VEC_OUTPUT_DIR, exist_ok=True)
client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = client.get_or_create_collection(name="quincy_vector_store")
model = SentenceTransformer(MODEL_NAME)
tracker = {}
hash_memory = {}

if os.path.exists(TRACKER_FILE):
    with open(TRACKER_FILE, "r", encoding="utf-8") as f:
        tracker = json.load(f)

if os.path.exists(HASH_LEDGER):
    with open(HASH_LEDGER, "r", encoding="utf-8") as f:
        hash_memory = json.load(f)

def hash_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

def log_progress(folder, file, status):
    if folder not in tracker:
        tracker[folder] = {}
    tracker[folder][file] = status
    with open(TRACKER_FILE, "w", encoding="utf-8") as f:
        json.dump(tracker, f, indent=2)

def log_global_hash(file_hash, path):
    hash_memory[file_hash] = path
    with open(HASH_LEDGER, "w", encoding="utf-8") as f:
        json.dump(hash_memory, f, indent=2)

def write_catalog_entry(meta):
    with open(CATALOG_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps(meta) + "\n")

def update_index_md(entries):
    with open(INDEX_MD, "a", encoding="utf-8") as f:
        for e in entries:
            f.write(f"- {e['uuid']} — {e['source_type']} — {e['path']}\n")

def embed_and_store(text, path, source_type):
    emb = model.encode([text])[0]
    uid = hashlib.sha256((path + text[:100]).encode()).hexdigest()
    file_hash = hash_file(path)
    collection.add(
        documents=[text],
        embeddings=[emb.tolist()],
        ids=[uid],
        metadatas=[{"source_type": source_type, "path": path}]
    )
    write_catalog_entry({"uuid": uid, "source_type": source_type, "path": path})
    update_index_md([{"uuid": uid, "source_type": source_type, "path": path}])
    log_global_hash(file_hash, path)

# === HANDLERS ===

def handle_txt(path):
    try:
        file_hash = hash_file(path)
        if file_hash in hash_memory:
            print(f"⏭️ Global duplicate skipped: {path}")
            log_progress(Path(path).parent.as_posix(), Path(path).name, "SKIPPED_DUPLICATE: hash-match")
            return
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        if not text.strip():
            print(f"⚠️ TXT file appears empty: {path}")
            return
        embed_and_store(text, path, "txt")
        print(f"📄 TXT processed: {path}")
    except Exception as e:
        print(f"❌ Error processing TXT {path}: {e}")

def handle_pdf(path):
    try:
        file_hash = hash_file(path)
        if file_hash in hash_memory:
            print(f"⏭️ Global duplicate skipped: {path}")
            log_progress(Path(path).parent.as_posix(), Path(path).name, "SKIPPED_DUPLICATE: hash-match")
            return
        doc = fitz.open(path)
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        if not text.strip():
            print(f"⚠️ PDF appears empty or unextractable: {path}")
            return
        embed_and_store(text, path, "pdf")
        print(f"📄 PDF extracted and embedded: {path}")
    except Exception as e:
        print(f"❌ Error processing PDF {path}: {e}")

def handle_docx(path):
    try:
        file_hash = hash_file(path)
        if file_hash in hash_memory:
            print(f"⏭️ Global duplicate skipped: {path}")
            log_progress(Path(path).parent.as_posix(), Path(path).name, "SKIPPED_DUPLICATE: hash-match")
            return
        doc = docx.Document(path)
        text = "\n".join([para.text for para in doc.paragraphs])
        if not text.strip():
            print(f"⚠️ DOCX appears empty: {path}")
            return
        embed_and_store(text, path, "docx")
        print(f"📄 DOCX extracted and embedded: {path}")
    except Exception as e:
        print(f"❌ Error processing DOCX {path}: {e}")

def handle_epub(path):
    try:
        file_hash = hash_file(path)
        if file_hash in hash_memory:
            print(f"⏭️ Global duplicate skipped: {path}")
            log_progress(Path(path).parent.as_posix(), Path(path).name, "SKIPPED_DUPLICATE: hash-match")
            return
        book = epub.read_epub(path)
        text = ""
        for item in book.get_items():
            if item.get_type() == epub.ITEM_DOCUMENT:
                soup = BeautifulSoup(item.get_content(), "html.parser")
                text += soup.get_text()
        if not text.strip():
            print(f"⚠️ EPUB appears empty: {path}")
            return
        embed_and_store(text, path, "epub")
        print(f"📚 EPUB extracted and embedded: {path}")
    except Exception as e:
        print(f"❌ Error processing EPUB {path}: {e}")

def handle_html(path):
    try:
        file_hash = hash_file(path)
        if file_hash in hash_memory:
            print(f"⏭️ Global duplicate skipped: {path}")
            log_progress(Path(path).parent.as_posix(), Path(path).name, "SKIPPED_DUPLICATE: hash-match")
            return
        with open(path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")
            text = soup.get_text()
        if not text.strip():
            print(f"⚠️ HTML appears empty: {path}")
            return
        embed_and_store(text, path, "html")
        print(f"🌐 HTML extracted and embedded: {path}")
    except Exception as e:
        print(f"❌ Error processing HTML {path}: {e}")

def handle_markdown(path):
    try:
        file_hash = hash_file(path)
        if file_hash in hash_memory:
            print(f"⏭️ Global duplicate skipped: {path}")
            log_progress(Path(path).parent.as_posix(), Path(path).name, "SKIPPED_DUPLICATE: hash-match")
            return
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        if not text.strip():
            print(f"⚠️ Markdown appears empty: {path}")
            return
        embed_and_store(text, path, "md")
        print(f"📝 Markdown extracted and embedded: {path}")
    except Exception as e:
        print(f"❌ Error processing Markdown {path}: {e}")

def handle_image(path):
    try:
        file_hash = hash_file(path)
        if file_hash in hash_memory:
            print(f"⏭️ Global duplicate skipped: {path}")
            log_progress(Path(path).parent.as_posix(), Path(path).name, "SKIPPED_DUPLICATE: hash-match")
            return
        text = pytesseract.image_to_string(Image.open(path))
        if not text.strip():
            print(f"⚠️ Image has no readable text: {path}")
            return
        embed_and_store(text, path, "image")
        print(f"🖼️ Image text extracted and embedded: {path}")
    except Exception as e:
        print(f"❌ Error processing Image {path}: {e}")

def handle_audio(path):
    try:
        file_hash = hash_file(path)
        if file_hash in hash_memory:
            print(f"⏭️ Global duplicate skipped: {path}")
            log_progress(Path(path).parent.as_posix(), Path(path).name, "SKIPPED_DUPLICATE: hash-match")
            return
        model = whisper.load_model("base")
        result = model.transcribe(path)
        text = result.get("text", "")
        if not text.strip():
            print(f"⚠️ Audio returned no transcript: {path}")
            return
        embed_and_store(text, path, "audio")
        print(f"🔊 Audio transcribed and embedded: {path}")
    except Exception as e:
        print(f"❌ Error processing Audio {path}: {e}")

def handle_video(path):
    try:
        file_hash = hash_file(path)
        if file_hash in hash_memory:
            print(f"⏭️ Global duplicate skipped: {path}")
            log_progress(Path(path).parent.as_posix(), Path(path).name, "SKIPPED_DUPLICATE: hash-match")
            return

        # Define temp .wav path
        filename_stem = Path(path).stem
        temp_wav_path = Path("E:/QuincyWebAccessMats/temp_processing") / f"{filename_stem}.wav"

        # Convert video audio to wav using ffmpeg
        command = [
            "ffmpeg",
            "-y",  # overwrite if exists
            "-i", str(path),
            "-vn",  # no video
            "-acodec", "pcm_s16le",
            "-ar", "44100",
            "-ac", "2",
            str(temp_wav_path)
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode != 0:
            print(f"❌ FFMPEG failed: {result.stderr.decode(errors='ignore')}")
            return

        # Transcribe with Whisper
        model = whisper.load_model("base")
        result = model.transcribe(str(temp_wav_path))
        text = result.get("text", "")
        if not text.strip():
            print(f"⚠️ Video audio returned no transcript: {path}")
            return

        embed_and_store(text, path, "video")
        print(f"🎞️ Video transcribed and embedded: {path}")

    except Exception as e:
        print(f"❌ Error processing Video {path}: {e}")
    finally:
        try:
            if temp_wav_path.exists():
                temp_wav_path.unlink()
        except Exception as cleanup_error:
            print(f"⚠️ Cleanup failed for {temp_wav_path}: {cleanup_error}")

def handle_archive(path):
    def recursive_extract(p):
        out_dir = os.path.splitext(str(p))[0] + "_extracted"
        subprocess.run([SEVEN_ZIP_PATH, "x", str(p), f"-o{out_dir}", "-y"], stdout=subprocess.PIPE)
        print(f"🗃️ Extracted: {p}")
        for root, _, files in os.walk(out_dir):
            for file in files:
                nested_path = Path(root) / file
                if nested_path.suffix.lower() in ['.zip', '.rar', '.7z']:
                    recursive_extract(nested_path)
    try:
        recursive_extract(path)
    except Exception as e:
        print(f"❌ Archive extraction failed: {path} — {e}")

EXTENSIONS = {
    '.txt': handle_txt,
    '.pdf': handle_pdf,
    '.docx': handle_docx,
    '.png': handle_image,
    '.jpg': handle_image,
    '.jpeg': handle_image,
    '.mp4': handle_video,
    '.mp3': handle_audio,
    '.wav': handle_audio,
    '.m4a': handle_audio,
    '.epub': handle_epub,
    '.html': handle_html,
    '.htm': handle_html,
    '.md': handle_markdown,
    '.zip': handle_archive,
    '.rar': handle_archive,
    '.7z': handle_archive
}

def process_folder(folder_path):
    print(f"\n📁 Scanning: {folder_path}\n")
    new_entries = 0
    for root, _, files in os.walk(folder_path):
        for name in files:
            file_path = str(Path(root) / name)
            ext = Path(name).suffix.lower()
            if ext in EXTENSIONS:
                file_hash = hash_file(file_path)
                if any(file_hash in entry for entry in tracker.get(folder_path, {}).values()):
                    print(f"⏭️ Skipping duplicate: {file_path}")
                    continue
                try:
                    EXTENSIONS[ext](file_path)
                    log_progress(folder_path, name, file_hash)
                    new_entries += 1
                except Exception as e:
                    log_progress(folder_path, name, f"ERROR: {e}")
                    print(f"❌ Failed: {file_path} — {e}")
    print(f"\n✅ {new_entries} new entries processed from {folder_path}")

if __name__ == "__main__":
    print("🔁 Resume State:\n")
    for folder, files in tracker.items():
        status = f"{len(files)} files tracked"
        print(f"🗂️ {folder} — {status}")

    while True:
        print("\n📂 Enter full input folder path or press Enter to quit:")
        src = input("> ").strip()
        if not src:
            break
        if not os.path.isdir(src):
            print("❌ Invalid path. Try again.")
            continue
        process_folder(src)

    print("\n🌙 Done. Pipeline standing by.")

