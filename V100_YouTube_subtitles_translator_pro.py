#!/usr/bin/env python3
"""
YouTube Transcript -> Translated SRT Converter (v3)

يحوّل ترجمة فيديو أو قائمة تشغيل يوتيوب إلى ملفي SRT: النسخة الأصلية
ونسخة مترجمة (العربية افتراضيًا)، مع دُفعات ترجمة متوازية وتكيّف
تلقائي مع الحمل (chunk size / worker count).

تشغيل تفاعلي (كما في النسخة الأصلية):
    python V2_YouTube_subtitles_translator_pro.py

تشغيل غير تفاعلي (مناسب للأتمتة / pipelines):
    python V2_YouTube_subtitles_translator_pro.py <رابط الفيديو أو القائمة> \
        [--target ar] [--workers 6] [--chunk-size 60] [--output-dir output] [--force]
"""
import argparse
import random
import re
import sys
import threading
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
    as_completed,
    wait,
)

import requests

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


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
OUTPUT_BASE_DIR = Path("output")
YOUTUBE_VIDEO_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{11}$")
LANG_CODE_PATTERN = re.compile(r"^[a-zA-Z]{2,3}(-[a-zA-Z]{2,8})?$")

# تبدأ قوية، ثم تقل تلقائيًا إذا ظهرت أخطاء
#
# إصلاح وقائي: كانت القيمة الافتراضية 60 سطرًا / 4000 حرف للدفعة
# الواحدة. المشكلة ليست في عدد الطلبات، بل في أن كل سطرين متتاليين
# داخل نفس الدفعة يشكّلان أحيانًا جملة واحدة مستمرة في اللغة الأصلية
# (شائع جدًا في ترجمات يوتيوب التلقائية)، فيدمجهما محرك الترجمة في جملة
# مترجمة واحدة ليخرج بترجمة سليمة لغويًا، فتختفي العلامة الفاصلة بين
# السطرين ويظهر أحدهما "مفقودًا" (رسائل [INFO] ... retrying individually).
# هذا سلوك متوقع من أي خدمة ترجمة نصوص عامة وليس عطلاً برمجيًا، ولا يمكن
# منعه 100% طالما نُرسل عدة أسطر في طلب واحد. لكن تقليل حجم الدفعة يقلّل
# عدد "الحدود الداخلية" المعرّضة للدمج في كل طلب، فيقلّل عدد المرات التي
# تحتاج فيها شبكة الأمان (إعادة الترجمة الفردية) للعمل من الأساس — وهذا
# يقلّل بدوره مدة التوقف الظاهري في نهاية كل فيديو.
TRANSLATION_CHUNK_SIZE = 40
TRANSLATION_MAX_CHARS_PER_CHUNK = 2500
TRANSLATION_WORKERS = 6
TRANSLATION_RETRIES = 4
TRANSLATION_REQUEST_TIMEOUT = 15     # ثانية: أقصى انتظار لطلب ترجمة واحد قبل اعتباره عالقًا
TRANSLATION_INITIAL_DELAY = 1.5      # ثانية قبل أول إعادة محاولة
TRANSLATION_BACKOFF_MULTIPLIER = 2.0  # معامل التضاعف الأسي بين المحاولات

# تأخير خفيف بين فيديوهات القائمة
PLAYLIST_VIDEO_DELAY = 2.0

_print_lock = threading.Lock()


def log(message: str) -> None:
    """طباعة آمنة بين الخيوط (Threads) لمنع تداخل الأسطر."""
    with _print_lock:
        print(message)


# ---------------------------------------------------------------------------
# تكيّف تلقائي وآمن بين الخيوط (Thread-safe adaptive throttling)
#
# ملاحظة إصلاح: في النسخة الأصلية كانت هذه الحالة عبارة عن متغيرات global
# يتم تعديلها من داخل خيوط ThreadPoolExecutor متعددة بدون أي قفل (Lock)،
# ما يسبب "race condition" حقيقية (فقدان تحديثات، عدّادات غير دقيقة).
# كما كانت note_translation_success() تُستدعى مرتين لكل دفعة ناجحة
# (مرة داخل translate_text ومرة أخرى داخل translate_chunk)، فتُضاعف
# عدّاد النجاح المتتالي وتُفسد منطق "كل 20 نجاحًا استقرار". الحل: كائن
# واحد محمي بقفل، ونقطة تسجيل واحدة فقط لكل نتيجة فعلية.
# ---------------------------------------------------------------------------
class AdaptiveThrottle:
    def __init__(self, chunk_size: int, workers: int):
        self._lock = threading.Lock()
        self.chunk_size = chunk_size
        self.workers = workers
        self._fail_streak = 0
        self._success_streak = 0

    def note_success(self) -> None:
        with self._lock:
            self._success_streak += 1
            self._fail_streak = 0
            # إصلاح: كان التعافي يحتاج 20 نجاحًا متتاليًا بدون أي فشل
            # واحد بينها لاستعادة worker واحد فقط، بينما التخفيض يحصل
            # بعد 3 محاولات فاشلة فقط ويخفّض 2 workers دفعة واحدة. هذا
            # التفاوت الكبير يعني أن أي عطل شبكة عابر (مثل "Network is
            # unreachable" اللحظي عند تبديل واي فاي/بيانات) في منتصف
            # فيديو طويل يُبقي البرنامج عالقًا بتزامن منخفض حتى نهاية
            # الفيديو بأكمله — حتى لو الشبكة رجعت ممتازة فورًا — لأن
            # إكمال 20-40 نجاحًا متتاليًا بلا أي انقطاع نادر الحدوث في
            # تشغيل طويل. تقليل العتبة إلى 8 يجعل البرنامج يستعيد
            # سرعته الكاملة خلال ثوانٍ من استقرار الشبكة الفعلي، بدل
            # أن يبقى بطيئًا بسبب عطل انتهى من زمن.
            if self._success_streak % 8 == 0:
                new_chunk = min(TRANSLATION_CHUNK_SIZE, self.chunk_size + 10)
                new_workers = min(TRANSLATION_WORKERS, self.workers + 1)
                if (new_chunk, new_workers) != (self.chunk_size, self.workers):
                    self.chunk_size, self.workers = new_chunk, new_workers
                    log(f"\n• استقرار الحمل → chunk={self.chunk_size}, workers={self.workers}")

    def note_failure(self) -> None:
        with self._lock:
            self._fail_streak += 1
            self._success_streak = 0
            if self._fail_streak % 3 == 0:
                new_chunk = max(10, self.chunk_size - 20)
                new_workers = max(1, self.workers - 2)
                if (new_chunk, new_workers) != (self.chunk_size, self.workers):
                    self.chunk_size, self.workers = new_chunk, new_workers
                    log(f"\n⚠️ تقليل الحمل → chunk={self.chunk_size}, workers={self.workers}")

    def snapshot(self) -> Tuple[int, int]:
        with self._lock:
            return self.chunk_size, self.workers


@dataclass
class RunConfig:
    """إعدادات التشغيل الحالية (تحل محل متغيرات عامة قابلة للتعديل)."""
    target_lang: str = "ar"
    force: bool = False
    prefer_english_source: bool = False
    throttle: AdaptiveThrottle = field(
        default_factory=lambda: AdaptiveThrottle(TRANSLATION_CHUNK_SIZE, TRANSLATION_WORKERS)
    )


# ---------------------------------------------------------------------------
# أدوات عامة (URL / ملفات / توقيت)
# ---------------------------------------------------------------------------
def get_video_title(video_id: str) -> str:
    try:
        url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
        # إصلاح: هذا الاستدعاء لا وجود له إطلاقًا في وضع ترجمة الملفات
        # المحلية (اختيار 2) — فهو موجود فقط هنا (اختيار 1) ويحدث بعد
        # انتهاء ترجمة كل فيديو مباشرة، فقط لجلب اسم مقروء لتسمية المجلد.
        # كانت مهلته 10 ثوانٍ، فإذا كانت الشبكة متقطعة (كما لاحظنا سابقًا
        # في السجلات) كان بإمكانه إضافة حتى 10 ثوانٍ توقف صامت لكل فيديو
        # دون أي علاقة بجوجل ترانسليت — وهذا جزء حقيقي من فرق السرعة بين
        # الخيارين. تقليل المهلة يجعله يفشل ويرجع لاستخدام معرّف الفيديو
        # كاسم بديل بسرعة أكبر بدل الانتظار الطويل.
        r = requests.get(url, timeout=5)
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
    # إصلاح: عناوين فيديوهات يوتيوب كثيرًا ما تحتوي على وسوم مثل "#شورتس"،
    # وكانت علامة # تمر دون تغيير لأنها غير موجودة ضمن قائمة الأحرف
    # الممنوعة أدناه، فتظهر داخل اسم المجلد الناتج. الحل: إزالتها تمامًا
    # (وليس استبدالها بشرطة سفلية كبقية الأحرف الممنوعة في أسماء الملفات).
    name = name.replace("#", "")
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

        result.append({"start": start, "duration": duration, "text": text})
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


def write_text_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    log(f"• Saved: {path}")


# ---------------------------------------------------------------------------
# قراءة ملف .srt موجود مسبقًا على الجهاز (ميزة اختيار ملفات الترجمة)
# ---------------------------------------------------------------------------
_SRT_TIME_RE = re.compile(r"(\d{1,2}):(\d{2}):(\d{2})[.,](\d{1,3})")
_SRT_ARROW_RE = re.compile(r"-->")


def _srt_timestamp_to_seconds(ts: str) -> float:
    m = _SRT_TIME_RE.search(ts)
    if not m:
        return 0.0
    h, mi, s, ms = m.groups()
    ms = ms.ljust(3, "0")[:3]  # يقبل ملي ثانية بأي عدد أرقام (1 إلى 3)
    return int(h) * 3600 + int(mi) * 60 + int(s) + int(ms) / 1000.0


def parse_srt_file(path: Path) -> List[Dict]:
    """
    يحوّل ملف .srt موجود على الجهاز إلى نفس صيغة المقاطع المستخدمة في
    باقي البرنامج (start / duration / text)، بحيث يمر عبر خط أنابيب
    الترجمة نفسه (الدفعات + إعادة المحاولة الفردية + الجولة الأخيرة)
    المستخدم مع ترجمات يوتيوب تمامًا — دون أي منطق مزدوج.

    يتعامل مع: BOM في بداية الملف، فواصل أسطر Windows (CRLF)، الفاصلة أو
    النقطة كفاصل للميلي ثانية، ووجود/غياب سطر رقم الترتيب قبل سطر التوقيت.
    """
    raw = path.read_text(encoding="utf-8-sig", errors="replace")
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    segments: List[Dict] = []
    blocks = re.split(r"\n\s*\n", raw.strip())

    for block in blocks:
        lines = [ln for ln in block.split("\n") if ln.strip() != ""]
        if not lines:
            continue

        time_line_idx = 0
        if _SRT_ARROW_RE.search(lines[0]) is None and lines[0].strip().isdigit():
            time_line_idx = 1  # السطر الأول رقم ترتيب الكتلة، وليس توقيتًا

        if time_line_idx >= len(lines) or _SRT_ARROW_RE.search(lines[time_line_idx]) is None:
            continue  # كتلة لا تحتوي سطر توقيت صالح — تُتجاهل بأمان

        start_str, _, end_str = lines[time_line_idx].partition("-->")
        start = _srt_timestamp_to_seconds(start_str.strip())
        end = _srt_timestamp_to_seconds(end_str.strip())

        text = " ".join(ln.strip() for ln in lines[time_line_idx + 1:]).strip()
        if not text:
            continue

        segments.append({"start": start, "duration": max(0.0, end - start), "text": text})

    return segments


# ---------------------------------------------------------------------------
# جلب الترجمة (Transcript) من يوتيوب
# ---------------------------------------------------------------------------
def get_transcript_list(video_id: str):
    """يدعم إصدارات youtube-transcript-api الحديثة والقديمة معًا."""
    ytt_api = YouTubeTranscriptApi()

    if hasattr(ytt_api, "list"):
        return ytt_api.list(video_id)

    if hasattr(YouTubeTranscriptApi, "list_transcripts"):
        return YouTubeTranscriptApi.list_transcripts(video_id)

    raise RuntimeError("Unsupported youtube-transcript-api version installed.")


def fetch_transcript(video_id: str, prefer_english_source: bool = False) -> Tuple[List[Dict], str]:
    """
    يجلب أفضل ترجمة متاحة لفيديو معيّن.

    إصلاح خطأ خفي: في النسخة الأصلية، إذا فشل جلب أول ترجمة أثناء
    المرور على transcript_list (مثلاً بسبب خطأ شبكة عابر)، كان الكود
    يوقف المحاولة بالكامل ويرفع استثناء فورًا دون تجربة بقية اللغات
    المتاحة. الآن يتم تجربة كل لغة متاحة بالترتيب، ولا نستسلم إلا
    بعد فشل جميعها.
    """
    log(f"• Fetching transcript for: {video_id}")

    try:
        transcript_list = get_transcript_list(video_id)
    except TranscriptsDisabled:
        raise RuntimeError("Transcripts are disabled for this video.")
    except Exception as exc:
        raise RuntimeError(f"Could not load transcript list: {exc}")

    if prefer_english_source:
        # المحاولة الأولى: تفضيل الترجمة الإنجليزية إن وُجدت
        try:
            transcript = transcript_list.find_transcript(["en"])
            raw = transcript.fetch()
            log("• English transcript found.")
            return normalize_segments(raw), "en"
        except NoTranscriptFound:
            log("  [INFO] English transcript not found. Falling back to available transcripts.")
        except Exception as exc:
            log(f"  [WARN] English transcript exists but failed to fetch: {exc}")

    # المحاولة الثانية: جرّب كل لغة متاحة، ولا تتوقف عند أول فشل
    last_error: Optional[Exception] = None
    tried_any = False
    for transcript in transcript_list:
        lang_code = getattr(transcript, "language_code", "unknown")
        tried_any = True
        try:
            raw = transcript.fetch()
            log(f"• Using transcript language: {lang_code}")
            return normalize_segments(raw), lang_code
        except Exception as exc:
            last_error = exc
            log(f"  [WARN] Failed to fetch '{lang_code}' transcript: {exc}")
            continue

    if last_error:
        raise RuntimeError(f"Could not fetch any transcript: {last_error}")
    if not tried_any:
        raise RuntimeError("No transcript available for this video.")
    raise RuntimeError("No transcript available for this video.")


# ---------------------------------------------------------------------------
# الترجمة (Translation)
# ---------------------------------------------------------------------------
def _translate_once(text: str, target_lang: str, timeout: float) -> str:
    """
    ينفّذ استدعاء GoogleTranslator().translate() الفعلي، لكن بسقف زمني
    صارم من عندنا (بدل ترك الأمر لمهلة نظام التشغيل/مكتبة الشبكة
    الداخلية، التي قد تكون طويلة جدًا أو غير محددة أصلًا).

    إصلاح جوهري: كانت هذه المكالمة تُنفَّذ مباشرة بلا أي timeout مضبوط
    من الكود على الإطلاق (لاحظ أن timeout=5 مضبوطة فقط على طلب عنوان
    الفيديو المنفصل، وليس هنا). عندما تنقطع الشبكة انقطاعًا "ناعمًا" —
    الاتصال يتعثر ولا يُرفض فورًا (بعكس "Network is unreachable" الذي
    يظهر بسرعة) — يبقى الخيط عالقًا بصمت تام، بلا أي سطر [WARN] يظهر،
    لأن كودنا لم يستلم أي استثناء ليطبعه بعد؛ هو ببساطة لا يزال منتظرًا.
    هذا بالضبط الفراغ الصامت الطويل الذي يظهر بالسجل قبل ظهور رسائل
    [WARN] بلحظات معدودة فقط قبل نهاية الفيديو.

    الحل: ننفّذ الاستدعاء الفعلي داخل خيط فرعي مستقل ونحدّ انتظارنا
    بمهلة صارمة (TRANSLATION_REQUEST_TIMEOUT). إن تجاوزها، نتخلى عن
    الانتظار فورًا (دون إيقاف الخيط الفرعي نفسه قسرًا — بايثون لا يسمح
    بذلك مباشرة، لكنه سيموت من تلقاء نفسه لاحقًا) ونُعامل الأمر كفشل
    عادي يدخل ضمن آلية إعادة المحاولة والـ backoff الحالية، بدل انتظار
    غير محدود قد يمتد لدقائق.
    """
    translator = GoogleTranslator(source="auto", target=target_lang)
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(translator.translate, text)
    try:
        return future.result(timeout=timeout)
    except FutureTimeoutError:
        executor.shutdown(wait=False)
        raise TimeoutError(f"Translation request stalled past {timeout:.0f}s (no response)")
    finally:
        executor.shutdown(wait=False)


def translate_text(
    text: str,
    throttle: AdaptiveThrottle,
    target_lang: str = "ar",
    retries: int = TRANSLATION_RETRIES,
) -> str:
    delay = TRANSLATION_INITIAL_DELAY

    for attempt in range(1, retries + 1):
        try:
            result = _translate_once(text, target_lang, TRANSLATION_REQUEST_TIMEOUT)
            throttle.note_success()
            return result if result else text
        except Exception as exc:
            throttle.note_failure()
            if attempt < retries:
                # إصلاح: كانت كل الخيوط الفاشلة في نفس اللحظة (وهو المتوقع
                # عند انقطاع شبكة عابر) تنام لنفس المدة بالضبط ثم تعيد
                # الطلب في نفس اللحظة أيضًا معًا، فتُصدم بموجة رفض جديدة
                # (thundering herd) — وهذا ما يبدو للمستخدم وكأن البرنامج
                # "توقف" لثوانٍ طويلة رغم أنه يعمل. إضافة عشوائية بسيطة
                # (jitter) على مدة الانتظار تُفرّق أوقات إعادة المحاولة بين
                # الخيوط فترتفع فرصة نجاح بعضها أبكر من غيره.
                sleep_for = delay + random.uniform(0, delay * 0.5)
                log(f"  [WARN] Translation failed: {exc} — retrying in {sleep_for:.1f}s")
                time.sleep(sleep_for)
                delay *= TRANSLATION_BACKOFF_MULTIPLIER
            else:
                log("  [WARN] Translation failed permanently. Keeping original text.")
                return text

    return text


def split_into_chunks(segments: List[Dict], chunk_size: int) -> List[List[Tuple[int, Dict]]]:
    """
    يقسّم المقاطع إلى دفعات حسب عدد الأسطر وعدد الأحرف الكلي.
    كل مقطع يحصل على معرّف عام (global id) ثابت.
    """
    chunks: List[List[Tuple[int, Dict]]] = []
    current: List[Tuple[int, Dict]] = []
    current_chars = 0

    for global_id, seg in enumerate(segments, start=1):
        seg_chars = max(len(seg["text"]), 1)
        would_exceed_count = len(current) >= chunk_size
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
    يبني طلب ترجمة واحد للدفعة كاملة باستخدام أسطر مرقّمة.
    الصيغة:
    000001 ||| original text
    000002 ||| original text
    """
    lines = []
    for seg_id, seg in chunk:
        text = seg["text"].replace("\n", " ").strip()
        lines.append(f"{seg_id:06d} ||| {text}")
    return "\n".join(lines)


# إصلاح خطأ خفي: كانت هذه الدالة تقسّم النص المترجم سطرًا بسطر وتطابق
# كل سطر على حدة (r"^(\d{6})\s*\|\|\|\s*(.*)$"). لكن خدمة الترجمة أحيانًا
# تُعيد تدفّق النص (تدمج سطرين في سطر واحد، أو تُدخل فاصل سطر إضافي
# داخل نص عنصر واحد)، فيفشل التطابق لذلك السطر تحديدًا ويختفي من
# الناتج الظاهر — وهذا بالضبط ما يظهر في الصورة المرفقة: سطر واحد بقي
# بالإنجليزية وسط بقية الأسطر المترجمة. الحل: مسح النص كاملاً دفعة
# واحدة (وليس سطرًا سطرًا) واعتبار حد كل عنصر هو ظهور العلامة التالية
# (نفس نمط الرقم + |||) أو نهاية النص، بدل الاعتماد على فواصل الأسطر.
_CHUNK_ENTRY_PATTERN = re.compile(r"(\d{6})\s*\|\|\|\s*(.*?)(?=\s*\d{6}\s*\|\|\||\Z)", re.S)


def parse_translated_chunk(translated_text: str, chunk: List[Tuple[int, Dict]]) -> Dict[int, str]:
    """يستعيد الأجزاء المترجمة باستخدام أرقام الأسطر الثابتة."""
    out: Dict[int, str] = {}

    for m in _CHUNK_ENTRY_PATTERN.finditer(translated_text):
        seg_id = int(m.group(1))
        text = re.sub(r"\s+", " ", m.group(2)).strip()
        if text:
            out[seg_id] = text

    return out


def translate_single_segment(
    seg_id: int, seg: Dict, throttle: AdaptiveThrottle, target_lang: str,
    retries: int = TRANSLATION_RETRIES,
) -> Tuple[int, Dict]:
    try:
        translated_text = translate_text(seg["text"], throttle, target_lang, retries=retries)
    except Exception:
        translated_text = seg["text"]

    return (
        seg_id,
        {"start": seg["start"], "duration": seg["duration"], "text": translated_text},
    )


def translate_chunk(
    chunk: List[Tuple[int, Dict]], throttle: AdaptiveThrottle, target_lang: str
) -> List[Tuple[int, Dict]]:
    """
    يترجم دفعة واحدة. إذا بدت الترجمة ضعيفة أو غير مكتملة، يُقسّم
    الدفعة إلى نصفين ويعيد المحاولة تكراريًا.

    ملاحظة إصلاح: كانت النسخة الأصلية تستدعي note_translation_success()
    مرة إضافية هنا حتى بعد أن استدعتها translate_text() بالفعل عند
    نجاح استدعاء الترجمة، ما يعني احتساب كل دفعة ناجحة كنجاحين اثنين
    ويُخلّ بعتبة "كل 20 نجاحًا" في AdaptiveThrottle. تم إزالة الاستدعاء
    المكرر؛ التسجيل الآن يحدث مرة واحدة فقط داخل translate_text لكل
    استدعاء فعلي لخدمة الترجمة.
    """
    if len(chunk) == 1:
        seg_id, seg = chunk[0]
        return [translate_single_segment(seg_id, seg, throttle, target_lang)]

    payload = build_chunk_payload(chunk)

    try:
        translated = translate_text(payload, throttle, target_lang)
        parsed = parse_translated_chunk(translated, chunk)

        if len(parsed) < max(1, int(len(chunk) * 0.8)):
            raise ValueError("Partial translation output")

        translated_chunk: List[Tuple[int, Dict]] = []
        missing_ids: List[int] = []
        for seg_id, seg in chunk:
            translated_text = parsed.get(seg_id, "").strip()
            if not translated_text:
                missing_ids.append(seg_id)
                translated_text = seg["text"]  # سيُستبدل أدناه إن نجحت إعادة المحاولة الفردية

            translated_chunk.append(
                (seg_id, {"start": seg["start"], "duration": seg["duration"], "text": translated_text})
            )

        # إصلاح خطأ خفي: عندما تتجاوز الدفعة عتبة الـ 80% نجاحًا لكن بعض
        # الأسطر (أقل من 20%) لا تزال مفقودة من الناتج المُحلَّل، كانت
        # هذه الأسطر بالتحديد تبقى بلغتها الأصلية دون ترجمة إلى الأبد —
        # لأن الدفعة كاملة تُعتبر "ناجحة" ولا تُعاد جدولتها أو تقسيمها.
        # هذا هو سبب ظهور سطر إنجليزي منفرد وسط سطور مترجمة في الصورة
        # المرفقة. الحل: أي سطر ناقص بعد التحليل الجماعي يُعاد ترجمته
        # بشكل مفرد (نص واحد فقط، بلا علامات ترقيم قابلة للتلف) بدل
        # تركه دون ترجمة.
        #
        # إصلاح إضافي (بطء ملحوظ): كانت هذه الإعادة تحدث بشكل متسلسل —
        # سطر تلو الآخر، كل واحد بطلب شبكة منفصل بمهلة إعادة محاولة
        # كاملة (حتى 4 محاولات بتأخير تصاعدي يصل مجموعه لنحو 15-20
        # ثانية للسطر الواحد وحده إن واجه فشلًا متكررًا). دفعة فيها 3
        # أسطر ناقصة كانت تُعيد ترجمتها بالتتابع، فتتراكم مدة الانتظار
        # (مجموع الأزمنة) بدل أن تتداخل. لا تُعاد ترجمة الدفعة كاملة —
        # فقط الأسطر الناقصة تحديدًا — لكن كل سطر منها كان يُعامل وكأنه
        # "الفرصة الأخيرة". الحل هنا مرحلتان: (1) تنفيذها بالتوازي بدل
        # التتابع (أقصى زمن لا مجموع الأزمنة)، و(2) تقليل عدد محاولات
        # إعادة المحاولة هنا تحديدًا (بدل الأربع الكاملة) لأنها ليست
        # الفرصة الأخيرة أصلاً — الجولة النهائية في translate_segments
        # هي خط الدفاع الأخير الحقيقي وتحتفظ بكل المحاولات الأربع.
        if missing_ids:
            log(f"  [INFO] {len(missing_ids)} line(s) missing from batch output — retrying individually.")
            seg_lookup = {seg_id: seg for seg_id, seg in chunk}
            fixup_workers = max(1, min(3, len(missing_ids)))
            with ThreadPoolExecutor(max_workers=fixup_workers) as fixup_executor:
                fixed = dict(
                    fixup_executor.map(
                        lambda sid: translate_single_segment(
                            sid, seg_lookup[sid], throttle, target_lang, retries=2
                        ),
                        missing_ids,
                    )
                )
            translated_chunk = [
                (seg_id, fixed.get(seg_id, data)) for seg_id, data in translated_chunk
            ]

        return translated_chunk

    except Exception as exc:
        throttle.note_failure()

        if len(chunk) > 1:
            mid = len(chunk) // 2
            left = chunk[:mid]
            right = chunk[mid:]
            log(f"⚠️ Splitting chunk ({len(chunk)}) → {len(left)} + {len(right)} | {exc}")
            return translate_chunk(left, throttle, target_lang) + translate_chunk(right, throttle, target_lang)

        seg_id, seg = chunk[0]
        return [translate_single_segment(seg_id, seg, throttle, target_lang)]


def translate_segments(segments: List[Dict], throttle: AdaptiveThrottle, target_lang: str) -> List[Dict]:
    if not segments:
        return []

    chunk_size, worker_limit = throttle.snapshot()
    chunks = split_into_chunks(segments, chunk_size)
    total_chunks = len(chunks)
    total_segments = len(segments)

    log(f"• Translating {total_segments} segments in {total_chunks} chunk(s)...")

    ordered_results: List[Optional[List[Tuple[int, Dict]]]] = [None] * total_chunks

    # إصلاح خطأ خفي: كانت كل الدُفعات تُرسل إلى ThreadPoolExecutor دفعة
    # واحدة بعدد عمّال ثابت مأخوذ من throttle مرة واحدة فقط قبل البدء.
    # AdaptiveThrottle مصمم ليقلّل عدد العمّال تلقائيًا عند تكرار الفشل،
    # لكن هذا التقليل كان لا يؤثر إلا على الفيديو *التالي*؛ فإذا انقطعت
    # الشبكة لحظيًا في منتصف فيديو طويل، كانت كل الخيوط الستة تستمر في
    # محاولة الاتصال بنفس الشدة حتى نهاية ذلك الفيديو تحديدًا — وهذا ما
    # يظهر للمستخدم وكأن البرنامج "يتوقف" لفترات طويلة رغم أنه يعمل.
    # الحل: إرسال الدُفعات على "موجات" وإعادة قراءة عدد العمّال الموصى
    # به من throttle قبل كل موجة، بحيث ينخفض التزامن فعليًا في نفس
    # الفيديو بمجرد أن تبدأ الإخفاقات بالتراكم.
    next_idx = 0
    done_segments = 0

    with ThreadPoolExecutor(max_workers=max(1, worker_limit)) as executor:
        pending: Dict[Future, int] = {}

        def top_up() -> None:
            nonlocal next_idx
            _, current_workers = throttle.snapshot()
            while len(pending) < max(1, current_workers) and next_idx < total_chunks:
                fut = executor.submit(translate_chunk, chunks[next_idx], throttle, target_lang)
                pending[fut] = next_idx
                next_idx += 1

        top_up()
        while pending:
            done, _ = wait(list(pending.keys()), return_when=FIRST_COMPLETED)
            for future in done:
                idx = pending.pop(future)
                try:
                    ordered_results[idx] = future.result()
                    done_segments += len(chunks[idx])
                    with _print_lock:
                        print(f"  translated {done_segments}/{total_segments}", end="\r")
                except Exception as exc:
                    log(f"\n  [WARN] Chunk {idx + 1} failed: {exc}")
                    ordered_results[idx] = [
                        (seg_id, {"start": seg["start"], "duration": seg["duration"], "text": seg["text"]})
                        for seg_id, seg in chunks[idx]
                    ]
            top_up()

    print()

    flat: List[Tuple[int, Dict]] = []
    for chunk_result in ordered_results:
        if chunk_result:
            flat.extend(chunk_result)

    flat.sort(key=lambda x: x[0])
    result_segments = [item[1] for item in flat]

    # إصلاح خطأ خفي: خط دفاع أخير ضد أي سطر يبقى دون ترجمة رغم كل ما
    # سبق (سواء بسبب فشل شبكة متكرر أو خلل تحليل نادر). أي مقطع خرج
    # بنفس نص المصدر بالضبط لم تنجح أي محاولة ترجمة سابقة له فعليًا،
    # فيُعاد بشكل متسلسل (عامل واحد فقط، بعد أن هدأ الحمل المتزامن) —
    # وهي بالضبط الحالة التي تظهر في لقطات الشاشة (سطر إنجليزي واحد وسط
    # سطور مترجمة).
    leftover = [
        (i, seg)
        for i, seg in enumerate(result_segments)
        if seg["text"].strip() and seg["text"].strip() == segments[i]["text"].strip()
    ]
    if leftover:
        # إصلاح جوهري: كانت هذه الجولة الأخيرة تُترجم كل سطر ناقص عبر
        # طلب شبكة منفصل بالكامل (translate_single_segment لكل سطر على
        # حدة) — يعني 49 سطرًا ناقصًا = 49 اتصالًا كاملاً بجوجل، كل واحد
        # يدفع تكلفة الاتصال/TLS/المعالجة بمفرده، حتى مع تنفيذها بالتوازي
        # (5 كحد أقصى) هذا يبقى بطيئًا جدًا (≈10 دفعات متتالية من 5).
        #
        # الحل: نجمّع الأسطر الناقصة في دفعات (chunks) بنفس آلية
        # الترجمة الأساسية (علامات "000123 ||| النص")، ونمررها لنفس
        # translate_chunk المستخدمة بالمرحلة الأولى — طلب واحد يترجم
        # عشرات الأسطر دفعة واحدة بدل عشرات الطلبات المنفردة.
        #
        # لماذا هذا آمن هنا تحديدًا رغم أن الدمج بين الأسطر هو ما سبّب
        # المشكلة أصلًا؟ لأن أسطر هذه الجولة متفرقة من أجزاء غير متتالية
        # من الفيديو (سطر من الدقيقة 3، آخر من الدقيقة 40، إلخ) ولا
        # تشكّل جملة واحدة متصلة عند وضعها جنبًا إلى جنب — فاحتمال أن
        # يدمجها محرك جوجل ببعضها ضعيف جدًا مقارنة بسطرين متجاورين
        # أصلًا في نفس الحوار. وإن ضاع أي سطر رغم ذلك، فـ translate_chunk
        # نفسها تكتشف هذا وتعيد ترجمته فرديًا بالتوازي تلقائيًا (نفس
        # آلية الحماية المستخدمة بالمرحلة الأولى بالضبط).
        _, current_workers = throttle.snapshot()
        final_workers = max(1, min(5, current_workers))
        final_batches = [
            leftover[i : i + TRANSLATION_CHUNK_SIZE]
            for i in range(0, len(leftover), TRANSLATION_CHUNK_SIZE)
        ]
        log(f"• Final pass: retrying {len(leftover)} still-untranslated line(s) "
            f"in {len(final_batches)} batch(es) ({final_workers} at a time)...")
        with ThreadPoolExecutor(max_workers=final_workers) as executor:
            futures = {
                executor.submit(translate_chunk, batch, throttle, target_lang): batch
                for batch in final_batches
            }
            for future in as_completed(futures):
                for seg_id, data in future.result():
                    result_segments[seg_id]["text"] = data["text"]
        print()

    return result_segments


# ---------------------------------------------------------------------------
# التعرّف على الرابط (فيديو مفرد أو قائمة تشغيل)
# ---------------------------------------------------------------------------
def _base_ydl_opts() -> Dict:
    return {
        "quiet": True,
        "extract_flat": True,
        "skip_download": True,
        "ignoreerrors": True,
        "noplaylist": False,
    }


def _extract_ids_from_entries(entries) -> List[str]:
    video_ids: List[str] = []
    for entry in entries:
        if not entry:
            continue

        candidate = entry.get("id") or entry.get("url") or entry.get("webpage_url")
        if not candidate:
            continue

        candidate = str(candidate)
        vid = extract_video_id(candidate)
        if not vid and YOUTUBE_VIDEO_ID_PATTERN.match(candidate):
            vid = candidate
        if vid:
            video_ids.append(vid)

    return video_ids


def detect_input_type(raw_input: str) -> Tuple[str, Optional[str], Optional[List[str]], Optional[str]]:
    """
    يُرجع:
      ("video", video_id, None, None)
      ("playlist", None, [video_ids], playlist_title)

    ملاحظة إصلاح: النسخة الأصلية كانت تحتوي دالة منفصلة
    get_playlist_video_ids() تُكرّر نفس منطق استخراج معرّفات الفيديو
    من نتيجة yt-dlp، لكنها لم تكن تُستدعى من أي مكان في البرنامج
    (كود ميت تمامًا). تم حذفها ودمج منطقها هنا عبر دالتين مساعدتين
    مشتركتين لتفادي الازدواجية.
    """
    raw_input = raw_input.strip()

    if YOUTUBE_VIDEO_ID_PATTERN.match(raw_input):
        return "video", raw_input, None, None

    direct_video_id = extract_video_id(raw_input)
    if direct_video_id:
        return "video", direct_video_id, None, None

    try:
        with yt_dlp.YoutubeDL(_base_ydl_opts()) as ydl:
            info = ydl.extract_info(raw_input, download=False)
    except Exception as exc:
        raise RuntimeError(f"Could not read input link: {exc}")

    if not info:
        raise RuntimeError("Could not detect whether the input is a video or playlist.")

    entries = info.get("entries")
    if entries:
        playlist_title = info.get("title") or "playlist"
        video_ids = _extract_ids_from_entries(entries)

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


# ---------------------------------------------------------------------------
# معالجة الفيديو / استئناف العمل (Resume)
# ---------------------------------------------------------------------------
def target_filename_for(target_lang: str) -> str:
    # نحافظ على اسم الملف الأصلي "arabic.srt" عند اللغة الافتراضية
    # للحفاظ على التوافق مع أي أتمتة تعتمد على هذا الاسم تحديدًا.
    return "arabic.srt" if target_lang == "ar" else f"{target_lang}.srt"


def find_existing_output_dir(base_dir: Path, video_id: str) -> Optional[Path]:
    """
    يبحث عن مجلد ناتج سابق لهذا الفيديو (بحسب اللاحقة _{video_id})
    لدعم استئناف معالجة قائمة تشغيل متوقفة دون إعادة كل شيء من الصفر.
    """
    if not base_dir.exists():
        return None
    for child in base_dir.iterdir():
        if child.is_dir() and (child.name == video_id or child.name.endswith(f"_{video_id}")):
            return child
    return None


def translate_srt_file(source_path: Path, target_path: Path, config: RunConfig) -> bool:
    """
    نقطة الترجمة الوحيدة في البرنامج لأي ملف .srt محلي: يقرأ source_path،
    يترجمه، ويحفظ الناتج في target_path.

    إصلاح بنيوي: كان عندنا سابقًا مساران منفصلان لنفس العملية بالضبط —
    واحد لفيديوهات يوتيوب (يترجم الـ segments في الذاكرة مباشرة بعد
    جلبها) وآخر للملفات المحلية (يقرأ .srt من القرص ثم يترجم). بما أن
    فيديوهات يوتيوب أصلاً تُحفظ كملف .srt محليًا فور جلبها (لعرضها
    للمستخدم بجانب الترجمة)، لا داعي إطلاقًا لمسارين مختلفين: الآن
    process_video يحفظ الأصل ثم يستدعي هذه الدالة بالضبط مثل
    process_local_subtitle_file تمامًا. الفائدة: سلوك ترجمة موحّد
    100% بين الخيارين (بدل نسختين قد تنحرف إحداهما عن الأخرى مستقبلاً)،
    وإذا انقطع الاتصال بيوتيوب أو أُعيد تشغيل السكربت بعد فشل الترجمة،
    النص الأصلي محفوظ محليًا بالفعل وجاهز للترجمة دون إعادة الاتصال
    بيوتيوب من جديد.
    """
    if not config.force and target_path.exists():
        log(f"• Skipping (already translated): {source_path.name} → {target_path}")
        return True

    log(f"• Reading subtitle file: {source_path.name}")
    try:
        segments = parse_srt_file(source_path)
    except Exception as exc:
        log(f"[SKIP] {source_path.name}: تعذّرت قراءة الملف ({exc})")
        return False

    if not segments:
        log(f"[SKIP] {source_path.name}: لم يتم العثور على أي مقاطع ترجمة صالحة داخل الملف")
        return False

    log(f"• {len(segments)} segment(s) found")

    translated_segments = translate_segments(segments, config.throttle, config.target_lang)
    translated_srt = build_srt(translated_segments)

    write_text_file(target_path, translated_srt)
    return True


def process_video(video_id: str, base_dir: Path, config: RunConfig) -> bool:
    effective_target_lang = "en" if config.prefer_english_source else config.target_lang
    target_filename = target_filename_for(effective_target_lang)

    if not config.force:
        existing_dir = find_existing_output_dir(base_dir, video_id)
        if existing_dir and (existing_dir / target_filename).exists():
            log(f"• Skipping (already translated): {video_id} → {existing_dir / target_filename}")
            return True

    try:
        segments, lang = fetch_transcript(video_id, prefer_english_source=config.prefer_english_source)
        log(f"• Source transcript language: {lang}")
    except Exception as exc:
        log(f"[SKIP] {video_id}: {exc}")
        return False

    title = get_video_title(video_id)
    safe_title = sanitize_filename(title, max_length=40)

    # اسم قصير وآمن لتجنب خطأ طول المسار
    out_dir = base_dir / f"{safe_title}_{video_id}" if safe_title else base_dir / video_id

    # احتياط إضافي لو بقي المسار طويلًا
    if len(str(out_dir)) > 160:
        out_dir = base_dir / video_id

    # نحفظ الترجمة الأصلية محليًا فورًا (قبل أي ترجمة)، ثم نمرّرها لنفس
    # دالة الترجمة المستخدمة تمامًا في وضع "اختيار ملفات من الجهاز".
    original_path = out_dir / f"{lang}.srt"
    write_text_file(original_path, build_srt(segments))

    target_path = out_dir / target_filename
    if config.prefer_english_source and lang.lower().startswith("en"):
        log("• English transcript is already available. Skipping translation step.")
        log(f"• Done: {title}")
        return True

    translation_config = replace(config, target_lang=effective_target_lang)
    ok = translate_srt_file(original_path, target_path, translation_config)
    if ok:
        log(f"• Done: {title}")
    return ok


def process_local_subtitle_file(path: Path, output_base_dir: Path, config: RunConfig) -> bool:
    """يترجم ملف ترجمة (.srt) موجود على الجهاز، ويحفظ الناتج بنفس اصطلاح
    تسمية الملفات المستخدم مع فيديوهات يوتيوب (arabic.srt أو {lang}.srt)."""
    target_filename = target_filename_for(config.target_lang)
    out_dir = output_base_dir / "local_subtitles" / (sanitize_filename(path.stem, max_length=60) or path.stem)
    target_path = out_dir / target_filename

    ok = translate_srt_file(path, target_path, config)
    if ok:
        log(f"• Done: {path.name}")
    return ok


# ---------------------------------------------------------------------------
# نقطة الدخول
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a YouTube video/playlist transcript into a translated SRT file."
    )
    parser.add_argument(
        "url", nargs="?", default=None, help="YouTube video/playlist URL or bare 11-char video ID"
    )
    parser.add_argument("--target", default="ar", help="Target language code for translation (default: ar)")
    parser.add_argument(
        "--workers", type=int, default=TRANSLATION_WORKERS, help="Max parallel translation workers"
    )
    parser.add_argument(
        "--chunk-size", type=int, default=TRANSLATION_CHUNK_SIZE, help="Initial segments per translation chunk"
    )
    parser.add_argument("--output-dir", default=str(OUTPUT_BASE_DIR), help="Base output directory")
    parser.add_argument(
        "--force", action="store_true", help="Re-process videos even if output already exists"
    )
    parser.add_argument(
        "--prefer-english-source",
        action="store_true",
        help="English-only mode for YouTube: use English transcript directly when available, otherwise translate available transcript to English",
    )
    return parser.parse_args(argv)


def build_run_config(args: argparse.Namespace) -> RunConfig:
    target_lang = args.target.strip().lower()
    if not LANG_CODE_PATTERN.match(target_lang):
        print(f"[ERROR] Invalid target language code: {args.target!r}")
        sys.exit(1)

    workers = max(1, args.workers)
    chunk_size = max(1, args.chunk_size)

    return RunConfig(
        target_lang=target_lang,
        force=args.force,
        prefer_english_source=args.prefer_english_source,
        throttle=AdaptiveThrottle(chunk_size, workers),
    )


def run(raw_input_value: str, output_base_dir: Path, config: RunConfig) -> int:
    try:
        input_type, video_id, video_ids, playlist_title = detect_input_type(raw_input_value)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 1

    if input_type == "video":
        if not video_id:
            print("[ERROR] Could not extract a valid YouTube video ID.")
            return 1

        out_dir = output_base_dir / "single_video"
        ok = process_video(video_id, out_dir, config)
        if not ok:
            return 1

        print("\nDone.")
        print(f"Output folder: {out_dir.resolve()}")
        return 0

    if input_type == "playlist":
        assert video_ids is not None
        assert playlist_title is not None

        safe_playlist_title = sanitize_filename(playlist_title, max_length=30)
        out_dir = output_base_dir / f"playlist_{safe_playlist_title}"

        print(f"• Playlist title: {playlist_title}")
        print(f"• Videos found: {len(video_ids)}")

        success = 0
        for i, vid in enumerate(video_ids, start=1):
            print(f"\n[{i}/{len(video_ids)}] Processing {vid}")
            if process_video(vid, out_dir, config):
                success += 1

            if i < len(video_ids):
                time.sleep(PLAYLIST_VIDEO_DELAY)

        print("\nFinished.")
        print(f"Successful videos: {success}/{len(video_ids)}")
        print(f"Output folder: {out_dir.resolve()}")
        return 0

    print("[ERROR] Unknown input type.")
    return 1


# ---------------------------------------------------------------------------
# وضع ترجمة ملفات محلية (اختيار ملف/ملفات .srt من الجهاز)
# ---------------------------------------------------------------------------
def prompt_target_language(default_lang: str) -> str:
    try:
        raw = input(f"لغة الترجمة الهدف (اضغط Enter لاستخدام '{default_lang}'): ").strip()
    except (EOFError, KeyboardInterrupt):
        raw = ""

    if not raw:
        return default_lang

    lang = raw.strip().lower()
    if not LANG_CODE_PATTERN.match(lang):
        log(f"[WARN] رمز لغة غير صالح: {raw!r} — سيتم استخدام '{default_lang}' بدلاً منه.")
        return default_lang

    return lang


def _guess_storage_roots() -> List[Path]:
    """أماكن شائعة لتخزين الملفات على أندرويد/لينكس، بترتيب الأولوية."""
    candidates = [
        Path("/storage/emulated/0"),
        Path("/sdcard"),
        Path.home(),
        Path.cwd(),
    ]
    return [p for p in candidates if p.exists()]


# إصلاح مهم: كانت هذه الميزة تعتمد على فتح نافذة اختيار ملفات عبر
# tkinter. تبيّن أن هذا يتسبب بانهيار فوري للتطبيق داخل Pydroid 3 على
# أندرويد (شاشة سوداء تظهر ثم تُغلق خلال أقل من ثانية) — لأن مكتبات
# Tcl/Tk الأصلية تحاول فتح اتصال بخادم عرض (X11/display) غير موجود على
# الجهاز، وهذا يحدث على مستوى منخفض (native) لا يمكن لبنية try/except في
# بايثون التقاطه؛ فالعملية كاملة تنهار قبل أن تصل حتى للسطر الذي يطبع أي
# رسالة خطأ. الحل: إزالة الاعتماد على tkinter نهائيًا، واستبداله بمتصفح
# ملفات نصي بسيط يعمل داخل الطرفية نفسها (نفس المكان الذي يعمل فيه باقي
# البرنامج أصلاً) — موثوق 100% لأنه لا يعتمد على أي مكتبة رسومية.
def browse_and_select_files() -> List[Path]:
    roots = _guess_storage_roots()
    current = roots[0] if roots else Path.cwd()
    selected: List[Path] = []

    print("\nمتصفح ملفات نصي — اختر ملف الترجمة (أو أكثر) من جهازك.")

    while True:
        try:
            entries = sorted(
                (p for p in current.iterdir() if not p.name.startswith(".")),
                key=lambda p: (p.is_file(), p.name.lower()),
            )
        except Exception as exc:
            log(f"[WARN] تعذّر فتح المجلد {current}: {exc}")
            entries = []

        print(f"\n📁 {current}")
        if selected:
            print(f"   (تم اختيار {len(selected)} ملف حتى الآن)")
        for i, p in enumerate(entries, start=1):
            print(f"  {i}) {'📁' if p.is_dir() else '📄'} {p.name}")

        print("  0) ⬆️  رجوع للمجلد الأعلى")
        print("  اكتب رقم ملف لإضافته (أو أرقام مفصولة بفواصل مثل 2,5,7)")
        print("  اكتب مسارًا كاملاً مباشرة للانتقال إليه أو لإضافته كملف")
        print("  اكتب 'تم' لإنهاء الاختيار والمتابعة")

        try:
            choice = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not choice:
            continue

        if choice in ("تم", "done", "d", "q"):
            break

        if choice == "0":
            current = current.parent
            continue

        parts = [p.strip() for p in choice.split(",") if p.strip()]

        # مسار كامل مكتوب يدويًا (وليس رقمًا من القائمة)
        if len(parts) == 1 and not parts[0].isdigit():
            candidate = Path(parts[0]).expanduser()
            if candidate.is_file():
                selected.append(candidate)
                print(f"  ✓ تمت إضافة: {candidate.name}")
            elif candidate.is_dir():
                current = candidate
            else:
                log(f"[WARN] المسار غير موجود: {candidate}")
            continue

        picked_any = False
        for part in parts:
            if not part.isdigit():
                continue
            idx = int(part) - 1
            if not (0 <= idx < len(entries)):
                log(f"[WARN] الرقم {part} غير موجود في القائمة الحالية.")
                continue

            target = entries[idx]
            if target.is_dir():
                if len(parts) == 1:
                    current = target
                else:
                    log(f"[WARN] تجاهلت '{target.name}' لأنه مجلد (اختره بمفرده للدخول إليه).")
                picked_any = True
            else:
                selected.append(target)
                print(f"  ✓ تمت إضافة: {target.name}")
                picked_any = True

        if not picked_any:
            log("[WARN] لم يتم التعرف على الإدخال — جرّب رقمًا من القائمة، أو مسارًا كاملًا، أو 'تم'.")

    return selected


def collect_subtitle_file_paths() -> List[Path]:
    return browse_and_select_files()


def run_local_subtitle_mode(output_base_dir: Path, config: RunConfig) -> int:
    paths = collect_subtitle_file_paths()
    if not paths:
        print("[ERROR] لم يتم اختيار أي ملف ترجمة.")
        return 1

    print(f"• {len(paths)} file(s) selected")

    target_lang = prompt_target_language(config.target_lang)
    if target_lang != config.target_lang:
        config = replace(config, target_lang=target_lang)

    out_dir = output_base_dir / "local_subtitles"
    success = 0
    for i, path in enumerate(paths, start=1):
        print(f"\n[{i}/{len(paths)}] Processing {path.name}")
        if process_local_subtitle_file(path, output_base_dir, config):
            success += 1

    print("\nFinished.")
    print(f"Successful files: {success}/{len(paths)}")
    print(f"Output folder: {out_dir.resolve()}")
    return 0


def main() -> None:
    args = parse_args()
    config = build_run_config(args)
    output_base_dir = Path(args.output_dir)

    print("YouTube Transcript → Translated SRT Converter")
    print("=" * 47)

    # لو مُرِّر رابط كوسيط سطر أوامر (استخدام آلي/سكربتات)، نحافظ على
    # السلوك القديم كما هو تمامًا ولا نعرض القائمة التفاعلية — لتفادي
    # كسر أي أتمتة تعتمد على الاستدعاء المباشر.
    if args.url:
        sys.exit(run(args.url, output_base_dir, config))

    print("اختر طريقة الاستخدام:")
    print("  1) إدخال رابط فيديو أو قائمة تشغيل يوتيوب")
    print("  2) اختيار ملف ترجمة (أو أكثر) من الجهاز لترجمته")

    try:
        choice = input("اكتب 1 أو 2 ثم اضغط Enter (الافتراضي 1): ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nAborted.")
        sys.exit(0)

    if choice == "2":
        sys.exit(run_local_subtitle_mode(output_base_dir, config))

    try:
        raw_input_value = input("Enter YouTube video or playlist URL: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\nAborted.")
        sys.exit(0)

    if not raw_input_value:
        print("[ERROR] No input provided.")
        sys.exit(1)

    exit_code = run(raw_input_value, output_base_dir, config)
    sys.exit(exit_code)


if __name__ == "__main__":
    # إصلاح: أي خطأ غير متوقع كان يمكن أن يُغلق نافذة Pydroid فورًا دون
    # أن يرى المستخدم أي رسالة (خصوصًا إن أنهى الاستثناء العملية بسرعة).
    # هذه الشبكة الأخيرة تطبع الخطأ كاملاً بوضوح ثم تنتظر ضغطة Enter قبل
    # الإغلاق، بدل اختفاء الشاشة السوداء خلال أقل من ثانية دون تفسير.
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        import traceback

        print("\n[FATAL ERROR] حدث خطأ غير متوقع أوقف البرنامج:")
        traceback.print_exc()
        try:
            input("\nاضغط Enter للخروج...")
        except (EOFError, KeyboardInterrupt):
            pass
        sys.exit(1)
