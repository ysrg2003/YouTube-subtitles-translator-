#!/usr/bin/env python3
import re
import sys
import time
import requests
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
except Exception:
    print("[ERROR] Missing dependency: youtube-transcript-api")
    print("Run: pip install youtube-transcript-api")
    sys.exit(1)

try:
    from deep_translator import GoogleTranslator
except Exception:
    print("[ERROR] Missing dependency: deep-translator")
    print("Run: pip install deep-translator")
    sys.exit(1)

try:
    import yt_dlp
except Exception:
    print("[ERROR] Missing dependency: yt-dlp")
    print("Run: pip install yt-dlp")
    sys.exit(1)


OUTPUT_BASE_DIR = Path("output")
YOUTUBE_VIDEO_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{11}$")

# تبدأ قوية، ثم تقل تلقائيًا إذا ظهرت أخطاء
TRANSLATION_CHUNK_SIZE = 100
TRANSLATION_MAX_CHARS_PER_CHUNK = 4500
TRANSLATION_WORKERS = 10
TRANSLATION_RETRIES = 4
TRANSLATION_BACKOFF = 1.5

# تقليل ضغط ديناميكي
DYNAMIC_CHUNK_SIZE = TRANSLATION_CHUNK_SIZE
DYNAMIC_WORKERS = TRANSLATION_WORKERS
FAIL_STREAK = 0
SUCCESS_STREAK = 0

# تأخير خفيف بين فيديوهات القائمة
PLAYLIST_VIDEO_DELAY = 2.0


def note_translation_success() -> None:
    global FAIL_STREAK, SUCCESS_STREAK, DYNAMIC_CHUNK_SIZE, DYNAMIC_WORKERS

    SUCCESS_STREAK += 1
    FAIL_STREAK = 0

    if SUCCESS_STREAK % 20 == 0:
        DYNAMIC_CHUNK_SIZE = min(TRANSLATION_CHUNK_SIZE, DYNAMIC_CHUNK_SIZE + 10)
        DYNAMIC_WORKERS = min(TRANSLATION_WORKERS, DYNAMIC_WORKERS + 1)
        print(f"\n• Load stabilized → chunk={DYNAMIC_CHUNK_SIZE}, workers={DYNAMIC_WORKERS}")


def note_translation_failure() -> None:
    global FAIL_STREAK, SUCCESS_STREAK, DYNAMIC_CHUNK_SIZE, DYNAMIC_WORKERS

    FAIL_STREAK += 1
    SUCCESS_STREAK = 0

    if FAIL_STREAK % 3 == 0:
        DYNAMIC_CHUNK_SIZE = max(10, DYNAMIC_CHUNK_SIZE - 20)
        DYNAMIC_WORKERS = max(1, DYNAMIC_WORKERS - 2)
        print(f"\n⚠️ Reducing load → chunk={DYNAMIC_CHUNK_SIZE}, workers={DYNAMIC_WORKERS}")


def get_video_title(video_id: str) -> str:
    try:
        url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
        r = requests.get(url, timeout=10)
        if r.ok:
            return r.json().get("title", video_id)
    except Exception:
        pass
    return video_id


def extract_video_id(url: str) -> Optional[str]:
    url = url.strip()

    if YOUTUBE_VIDEO_ID_PATTERN.match(url):
        return url

    try:
        parsed = urlparse(url)
    except Exception:
        return None

    hostname = (parsed.hostname or "").lower()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    if hostname.startswith("m."):
        hostname = hostname[2:]

    if hostname == "youtu.be":
        vid_id = parsed.path.lstrip("/").split("/")[0].split("?")[0]
        return vid_id if YOUTUBE_VIDEO_ID_PATTERN.match(vid_id) else None

    if hostname not in ("youtube.com", "youtube-nocookie.com"):
        return None

    path_parts = [p for p in parsed.path.split("/") if p]

    if path_parts and path_parts[0] in ("embed", "v", "shorts", "e"):
        if len(path_parts) >= 2:
            vid_id = path_parts[1].split("?")[0]
            return vid_id if YOUTUBE_VIDEO_ID_PATTERN.match(vid_id) else None

    qs = parse_qs(parsed.query)
    if "v" in qs:
        vid_id = qs["v"][0]
        return vid_id if YOUTUBE_VIDEO_ID_PATTERN.match(vid_id) else None

    return None


def sanitize_filename(name: str, max_length: int = 80) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "_", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name[:max_length].rstrip(" ._")


def seconds_to_srt_timestamp(seconds: float) -> str:
    total_ms = int(round(seconds * 1000))
    ms = total_ms % 1000
    total_s = total_ms // 1000
    s = total_s % 60
    total_m = total_s // 60
    m = total_m % 60
    h = total_m // 60
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def normalize_segments(raw_segments) -> List[Dict]:
    result = []
    for item in raw_segments:
        if isinstance(item, dict):
            start = float(item.get("start", 0))
            duration = float(item.get("duration", 0))
            text = str(item.get("text", "")).strip()
        else:
            start = float(getattr(item, "start", 0))
            duration = float(getattr(item, "duration", 0))
            text = str(getattr(item, "text", "")).strip()

        result.append(
            {
                "start": start,
                "duration": duration,
                "text": text,
            }
        )
    return result


def build_srt(segments: List[Dict]) -> str:
    blocks = []
    for idx, seg in enumerate(segments, start=1):
        start_ts = seconds_to_srt_timestamp(seg["start"])
        end_ts = seconds_to_srt_timestamp(seg["start"] + seg.get("duration", 0))
        text = seg["text"].replace("\n", " ").strip()
        block = f"{idx}\n{start_ts} --> {end_ts}\n{text}\n"
        blocks.append(block)
    return "\n".join(blocks)


def get_transcript_list(video_id: str):
    """
    Support both current and older youtube-transcript-api versions.
    """
    ytt_api = YouTubeTranscriptApi()

    if hasattr(ytt_api, "list"):
        return ytt_api.list(video_id)

    if hasattr(YouTubeTranscriptApi, "list_transcripts"):
        return YouTubeTranscriptApi.list_transcripts(video_id)

    raise RuntimeError("Unsupported youtube-transcript-api version installed.")


def fetch_transcript(video_id: str) -> tuple[List[Dict], str]:
    print(f"• Fetching transcript for: {video_id}")

    try:
        transcript_list = get_transcript_list(video_id)
    except TranscriptsDisabled:
        raise RuntimeError("Transcripts are disabled for this video.")
    except Exception as exc:
        raise RuntimeError(f"Could not load transcript list: {exc}")

    try:
        transcript = transcript_list.find_transcript(["en"])
        print("• English transcript found.")
        raw = transcript.fetch()
        return normalize_segments(raw), "en"
    except NoTranscriptFound:
        pass
    except Exception:
        pass

    try:
        for transcript in transcript_list:
            lang_code = getattr(transcript, "language_code", "unknown")
            print(f"• Using transcript language: {lang_code}")
            raw = transcript.fetch()
            return normalize_segments(raw), lang_code
    except Exception as exc:
        raise RuntimeError(f"Could not fetch any transcript: {exc}")

    raise RuntimeError("No transcript available for this video.")


def translate_text(text: str, retries: int = TRANSLATION_RETRIES) -> str:
    delay = TRANSLATION_BACKOFF

    for attempt in range(1, retries + 1):
        try:
            translator = GoogleTranslator(source="auto", target="ar")
            result = translator.translate(text)
            note_translation_success()
            return result if result else text
        except Exception as exc:
            note_translation_failure()
            if attempt < retries:
                print(f"  [WARN] Translation failed: {exc} — retrying in {delay:.1f}s")
                time.sleep(delay)
                delay *= 2
            else:
                print("  [WARN] Translation failed permanently. Keeping original text.")
                return text

    return text


def split_into_chunks(segments: List[Dict]) -> List[List[Tuple[int, Dict]]]:
    """
    Split into chunks by number of segments and total character count.
    Each segment gets a stable global id.
    """
    chunks: List[List[Tuple[int, Dict]]] = []
    current: List[Tuple[int, Dict]] = []
    current_chars = 0

    for global_id, seg in enumerate(segments, start=1):
        seg_chars = max(len(seg["text"]), 1)
        would_exceed_count = len(current) >= DYNAMIC_CHUNK_SIZE
        would_exceed_chars = current and (current_chars + seg_chars > TRANSLATION_MAX_CHARS_PER_CHUNK)

        if current and (would_exceed_count or would_exceed_chars):
            chunks.append(current)
            current = [(global_id, seg)]
            current_chars = seg_chars
        else:
            current.append((global_id, seg))
            current_chars += seg_chars

    if current:
        chunks.append(current)

    return chunks


def build_chunk_payload(chunk: List[Tuple[int, Dict]]) -> str:
    """
    Build one request for the whole chunk using numbered lines.
    Format:
    000001 ||| original text
    000002 ||| original text
    """
    lines = []
    for seg_id, seg in chunk:
        text = seg["text"].replace("\n", " ").strip()
        lines.append(f"{seg_id:06d} ||| {text}")
    return "\n".join(lines)


def parse_translated_chunk(translated_text: str, chunk: List[Tuple[int, Dict]]) -> Dict[int, str]:
    """
    Recover translated pieces by using the stable line numbers.
    """
    out: Dict[int, str] = {}

    for line in translated_text.splitlines():
        line = line.strip()
        m = re.match(r"^(\d{6})\s*\|\|\|\s*(.*)$", line)
        if not m:
            continue
        seg_id = int(m.group(1))
        text = m.group(2).strip()
        out[seg_id] = text

    return out


def translate_single_segment(seg_id: int, seg: Dict) -> Tuple[int, Dict]:
    try:
        translated_text = translate_text(seg["text"])
    except Exception:
        translated_text = seg["text"]

    return (
        seg_id,
        {
            "start": seg["start"],
            "duration": seg["duration"],
            "text": translated_text,
        },
    )


def translate_chunk(chunk: List[Tuple[int, Dict]]) -> List[Tuple[int, Dict]]:
    """
    Translate one chunk.
    If the chunk translation looks weak or malformed, split into halves recursively.
    """
    if len(chunk) == 1:
        seg_id, seg = chunk[0]
        return [translate_single_segment(seg_id, seg)]

    payload = build_chunk_payload(chunk)

    try:
        translated = translate_text(payload)
        parsed = parse_translated_chunk(translated, chunk)

        if len(parsed) < max(1, int(len(chunk) * 0.8)):
            raise ValueError("Partial translation output")

        translated_chunk: List[Tuple[int, Dict]] = []
        for seg_id, seg in chunk:
            translated_text = parsed.get(seg_id, "").strip()
            if not translated_text:
                translated_text = seg["text"]

            translated_chunk.append(
                (
                    seg_id,
                    {
                        "start": seg["start"],
                        "duration": seg["duration"],
                        "text": translated_text,
                    },
                )
            )

        note_translation_success()
        return translated_chunk

    except Exception as exc:
        note_translation_failure()

        if len(chunk) > 1:
            mid = len(chunk) // 2
            left = chunk[:mid]
            right = chunk[mid:]
            print(f"⚠️ Splitting chunk ({len(chunk)}) → {len(left)} + {len(right)} | {exc}")
            return translate_chunk(left) + translate_chunk(right)

        seg_id, seg = chunk[0]
        return [translate_single_segment(seg_id, seg)]


def translate_segments_to_arabic(segments: List[Dict]) -> List[Dict]:
    if not segments:
        return []

    chunks = split_into_chunks(segments)
    total_chunks = len(chunks)
    total_segments = len(segments)

    print(f"• Translating {total_segments} segments in {total_chunks} chunk(s)...")

    worker_count = min(DYNAMIC_WORKERS, total_chunks)
    ordered_results: List[List[Tuple[int, Dict]]] = [None] * total_chunks  # type: ignore

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {executor.submit(translate_chunk, chunk): idx for idx, chunk in enumerate(chunks)}

        done_segments = 0
        for future in as_completed(future_map):
            idx = future_map[future]
            try:
                ordered_results[idx] = future.result()
                done_segments += len(chunks[idx])
                print(f"  translated {done_segments}/{total_segments}", end="\r")
            except Exception as exc:
                print(f"\n  [WARN] Chunk {idx + 1} failed: {exc}")
                ordered_results[idx] = [
                    (
                        seg_id,
                        {
                            "start": seg["start"],
                            "duration": seg["duration"],
                            "text": seg["text"],
                        },
                    )
                    for seg_id, seg in chunks[idx]
                ]

    print()

    flat: List[Tuple[int, Dict]] = []
    for chunk_result in ordered_results:
        if chunk_result:
            flat.extend(chunk_result)

    flat.sort(key=lambda x: x[0])
    return [item[1] for item in flat]


def write_text_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(f"• Saved: {path}")


def validate_url_or_exit(raw_url: str) -> str:
    video_id = extract_video_id(raw_url)
    if not video_id:
        print("\n[ERROR] Could not extract a valid YouTube video ID.")
        print("Accepted formats include:")
        print("  https://www.youtube.com/watch?v=XXXXXXXXXXX")
        print("  https://youtu.be/XXXXXXXXXXX")
        print("  https://youtube.com/embed/XXXXXXXXXXX")
        print("  https://m.youtube.com/watch?v=XXXXXXXXXXX")
        print("  https://www.youtube.com/v/XXXXXXXXXXX")
        print("  XXXXXXXXXXX  (bare video ID)")
        sys.exit(1)
    return video_id


def get_playlist_video_ids(playlist_url: str) -> Tuple[str, List[str]]:
    """
    Extract video IDs from a YouTube playlist using yt-dlp.
    """
    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
        "ignoreerrors": True,
        "noplaylist": False,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(playlist_url, download=False)
    except Exception as exc:
        raise RuntimeError(f"Could not read playlist: {exc}")

    if not info:
        raise RuntimeError("Playlist is empty or could not be loaded.")

    playlist_title = info.get("title") or "playlist"
    entries = info.get("entries") or []

    video_ids: List[str] = []
    for entry in entries:
        if not entry:
            continue

        candidate = (
            entry.get("id")
            or entry.get("url")
            or entry.get("webpage_url")
            or entry.get("ie_key")
        )

        if not candidate:
            continue

        candidate = str(candidate)
        vid = extract_video_id(candidate) or (candidate if YOUTUBE_VIDEO_ID_PATTERN.match(candidate) else None)
        if vid:
            video_ids.append(vid)

    if not video_ids:
        raise RuntimeError("No valid video IDs found inside the playlist.")

    return playlist_title, video_ids


def detect_input_type(raw_input: str) -> Tuple[str, Optional[str], Optional[List[str]], Optional[str]]:
    """
    Returns:
      ("video", video_id, None, None)
      ("playlist", None, [video_ids], playlist_title)
    """
    raw_input = raw_input.strip()

    if YOUTUBE_VIDEO_ID_PATTERN.match(raw_input):
        return "video", raw_input, None, None

    direct_video_id = extract_video_id(raw_input)
    if direct_video_id:
        return "video", direct_video_id, None, None

    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
        "ignoreerrors": True,
        "noplaylist": False,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(raw_input, download=False)
    except Exception as exc:
        raise RuntimeError(f"Could not read input link: {exc}")

    if not info:
        raise RuntimeError("Could not detect whether the input is a video or playlist.")

    entries = info.get("entries")
    if entries:
        playlist_title = info.get("title") or "playlist"
        video_ids: List[str] = []

        for entry in entries:
            if not entry:
                continue

            candidate = (
                entry.get("id")
                or entry.get("url")
                or entry.get("webpage_url")
            )

            if not candidate:
                continue

            candidate = str(candidate)
            vid = extract_video_id(candidate)
            if vid:
                video_ids.append(vid)
            elif YOUTUBE_VIDEO_ID_PATTERN.match(candidate):
                video_ids.append(candidate)

        if not video_ids:
            raise RuntimeError("Playlist detected, but no valid video IDs were found.")

        return "playlist", None, video_ids, playlist_title

    video_id = info.get("id")
    if video_id and YOUTUBE_VIDEO_ID_PATTERN.match(video_id):
        return "video", video_id, None, None

    fallback_video_id = extract_video_id(raw_input)
    if fallback_video_id:
        return "video", fallback_video_id, None, None

    raise RuntimeError("Input was detected, but could not be resolved as a video or playlist.")


def process_video(video_id: str, base_dir: Path) -> bool:
    try:
        segments, lang = fetch_transcript(video_id)
        print(f"• Source transcript language: {lang}")
    except Exception as exc:
        print(f"[SKIP] {video_id}: {exc}")
        return False

    original_srt = build_srt(segments)
    arabic_segments = translate_segments_to_arabic(segments)
    arabic_srt = build_srt(arabic_segments)

    title = get_video_title(video_id)
    safe_title = sanitize_filename(title, max_length=40)

    # اسم قصير وآمن لتجنب خطأ طول المسار
    if safe_title:
        out_dir = base_dir / f"{safe_title}_{video_id}"
    else:
        out_dir = base_dir / video_id

    # احتياط إضافي لو بقي المسار طويلًا
    if len(str(out_dir)) > 160:
        out_dir = base_dir / video_id

    write_text_file(out_dir / f"{lang}.srt", original_srt)
    write_text_file(out_dir / "arabic.srt", arabic_srt)

    print(f"• Done: {title}")
    return True


def main() -> None:
    print("YouTube Transcript → Arabic SRT Converter")
    print("=" * 45)

    try:
        raw_input_value = input("Enter YouTube video or playlist URL: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nAborted.")
        sys.exit(0)

    if not raw_input_value:
        print("[ERROR] No input provided.")
        sys.exit(1)

    try:
        input_type, video_id, video_ids, playlist_title = detect_input_type(raw_input_value)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)

    if input_type == "video":
        if not video_id:
            print("[ERROR] Could not extract a valid YouTube video ID.")
            sys.exit(1)

        out_dir = OUTPUT_BASE_DIR / "single_video"
        ok = process_video(video_id, out_dir)
        if not ok:
            sys.exit(1)

        print("\nDone.")
        print(f"Output folder: {out_dir.resolve()}")
        return

    if input_type == "playlist":
        assert video_ids is not None
        assert playlist_title is not None

        safe_playlist_title = sanitize_filename(playlist_title, max_length=30)
        out_dir = OUTPUT_BASE_DIR / f"playlist_{safe_playlist_title}"

        print(f"• Playlist title: {playlist_title}")
        print(f"• Videos found: {len(video_ids)}")

        success = 0
        for i, vid in enumerate(video_ids, start=1):
            print(f"\n[{i}/{len(video_ids)}] Processing {vid}")
            if process_video(vid, out_dir):
                success += 1

            if i < len(video_ids):
                time.sleep(PLAYLIST_VIDEO_DELAY)

        print("\nFinished.")
        print(f"Successful videos: {success}/{len(video_ids)}")
        print(f"Output folder: {out_dir.resolve()}")
        return

    print("[ERROR] Unknown input type.")
    sys.exit(1)


if __name__ == "__main__":
    main()