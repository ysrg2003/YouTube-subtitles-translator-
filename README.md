# YouTube Subtitles Translator Pro (Python)

أداة بايثون لاستخراج ترجمات YouTube وتحويلها إلى ملفات `.srt` مترجمة تلقائيًا (العربية افتراضيًا)، مع دعم الفيديو المفرد وقوائم التشغيل.  
Python tool to fetch YouTube transcripts and generate translated `.srt` files (Arabic by default), supporting both single videos and playlists.

> Repository: `ysrg2003/YouTube-subtitles-translator-`  
> Default branch: `main`  
> Primary language: Python

---

## نظرة عامة | Project Overview

**العربية:**  
هذا المشروع مناسب للمتعلمين، صناع المحتوى، والمستخدمين الذين يريدون ترجمة ترجمات فيديوهات YouTube بسرعة وبشكل منظم، خصوصًا عند التعامل مع فيديوهات طويلة أو قوائم تشغيل كاملة.

**English:**  
This project is useful for learners, content creators, and anyone who wants fast, structured subtitle translation from YouTube—especially for long videos or full playlists.

### متى تستخدمه؟ | When to use it
- عندما تريد ملف ترجمة مترجم أوفلاين بصيغة `.srt`.
- عندما تحتاج معالجة قائمة تشغيل كاملة بدلًا من فيديو واحد.
- عندما تريد التحكم في لغة الهدف، عدد العمال (`workers`)، وحجم الدفعة (`chunk-size`).

---

## الميزات | Features

- استخراج الترجمة الأصلية من YouTube مع التوقيتات (timestamps).
- ترجمة تلقائية عبر `deep-translator` (GoogleTranslator).
- دعم إدخال:
  - رابط فيديو YouTube
  - Video ID فقط (11 characters)
  - رابط Playlist
- معالجة متوازية مع تكيّف تلقائي لتقليل الأخطاء عند ضغط الترجمة.
- حفظ النسخة الأصلية + النسخة المترجمة في بنية ملفات واضحة.
- تخطي الفيديوهات المترجمة سابقًا تلقائيًا (إلا عند استخدام `--force`).
- وضع تفاعلي، ووضع سطر أوامر مناسب للأتمتة.

---

## المتطلبات | Prerequisites

- Python 3.9+ (يُفضّل)
- اتصال إنترنت
- الحزم التالية:

```bash
pip install youtube-transcript-api deep-translator yt-dlp requests
```

> ملاحظة: في وضع اختيار ملفات `.srt` محليًا (واجهة الملفات)، قد تحتاج بيئة تدعم `tkinter` حسب نظامك.

---

## التثبيت والإعداد | Installation & Setup

1. ادخل إلى مجلد المشروع:

```bash
cd /path/to/YouTube-subtitles-translator-
```

2. (اختياري لكن مُوصى به) أنشئ بيئة افتراضية:

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
```

3. ثبّت الاعتمادات:

```bash
pip install youtube-transcript-api deep-translator yt-dlp requests
```

---

## التشغيل | Usage

اسم السكربت الحالي داخل المستودع:

```bash
python V100_YouTube_subtitles_translator_pro.py
```

## GitHub Actions (تشغيل عبر GitHub)

تمت إضافة Workflow جاهز في:

```text
.github/workflows/run-translator.yml
```

لتشغيله:
- افتح تبويب **Actions** في GitHub.
- اختر **Run YouTube Subtitles Translator**.
- اضغط **Run workflow** وأدخل:
  - `source`: رابط فيديو/Playlist أو Video ID
  - `target` (اختياري، افتراضي `ar`)
  - `workers` (اختياري)
  - `chunk_size` (اختياري)
  - `force` (اختياري)
  - `prefer_english_source` (اختياري): وضع إنجليزي فقط؛ إذا وُجدت ترجمة إنجليزية يحفظها مباشرة، وإن لم توجد يترجم أي ترجمة متاحة إلى الإنجليزية

إذا واجهت حظر IP من YouTube داخل GitHub Actions:
- أضف Secret باسم `SCRAPERAPI_KEY` في إعدادات المستودع.
- الـ workflow يمرّر هذا المفتاح تلقائيًا للسكربت.
- عند وجود المفتاح، السكربت يفعّل proxy عبر ScraperAPI لطلبات transcript.

بعد اكتمال التشغيل ستجد النتائج في **Artifacts** باسم:
- `translated-subtitles`

### 1) الوضع التفاعلي | Interactive mode

شغّل السكربت بدون معاملات ثم اختر:
- `1` لإدخال رابط فيديو/Playlist
- `2` لترجمة ملفات `.srt` محلية

### 2) وضع سطر الأوامر | CLI mode

```bash
python V100_YouTube_subtitles_translator_pro.py "<video_or_playlist_url_or_video_id>" \
  --target ar \
  --workers 6 \
  --chunk-size 40 \
  --output-dir output \
  --prefer-english-source
```

#### الخيارات | Options
- `--target`: كود لغة الهدف (افتراضي: `ar`)
- `--workers`: عدد عمليات الترجمة المتوازية
- `--chunk-size`: حجم دفعة المقاطع المترجمة
- `--output-dir`: مجلد المخرجات الأساسي
- `--force`: إعادة المعالجة حتى إذا كانت الترجمة موجودة
- `--prefer-english-source`: وضع إنجليزي فقط (لفيديوهات YouTube): يحفظ الإنجليزية مباشرة إن كانت متاحة، وإلا يترجم الترجمة المتاحة إلى الإنجليزية
  - عند تفعيل هذا الخيار، يكون ملف الخرج النهائي إنجليزيًا (`en.srt`) حتى لو كانت `--target` مختلفة.

---

## المدخلات المتوقعة | Supported Inputs

يمكن إدخال أي من التالي:

1. **YouTube video URL**  
   مثال: `https://www.youtube.com/watch?v=dQw4w9WgXcQ`

2. **YouTube video ID** (11 chars)  
   مثال: `dQw4w9WgXcQ`

3. **YouTube playlist URL**  
   مثال: `https://www.youtube.com/playlist?list=PLxxxxxx`

---

## شكل المخرجات | Output Structure

افتراضيًا تُحفظ النتائج داخل `output/` (أو المسار المحدد عبر `--output-dir`).

```text
output/
├── single_video/
│   └── <video_title_or_id>_<video_id>/
│       ├── <source_lang>.srt
│       └── arabic.srt أو <target_lang>.srt
├── playlist_<playlist_title>/
│   └── <video_title_or_id>_<video_id>/
│       ├── <source_lang>.srt
│       └── arabic.srt أو <target_lang>.srt
└── local_subtitles/
    └── <subtitle_file_name>/
        └── arabic.srt أو <target_lang>.srt
```

---

## ملاحظات مهمة وسلوك التشغيل | Runtime Notes & Limitations

- يعتمد المشروع على توفر Transcript للفيديو في YouTube؛ بعض الفيديوهات قد تفشل (مثل تعطيل الترجمة أو عدم وجودها).
- جودة الترجمة تعتمد على خدمة الترجمة الآلية وقد تحتاج مراجعة بشرية.
- توجد مهلة قصيرة بين فيديوهات Playlist لتقليل الضغط على الخدمات.
- السكربت يحاول التكيّف تلقائيًا (تقليل/زيادة `workers` و`chunk-size`) عند نجاح/فشل الطلبات.
- إذا كانت الترجمة موجودة مسبقًا، يتم التخطي تلقائيًا ما لم تستخدم `--force`.
- يمكن تفعيل تجاوز حظر IP عبر ScraperAPI بوضع متغير البيئة `SCRAPERAPI_KEY` (اختياري).

---

## مثال سريع | Quick Example

```bash
python V100_YouTube_subtitles_translator_pro.py "https://www.youtube.com/watch?v=dQw4w9WgXcQ" --target ar
```

الناتج: مجلد داخل `output/single_video/` يحتوي النسخة الأصلية ونسخة مترجمة بصيغة `.srt`.
