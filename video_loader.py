# pip install -U yt-dlp ffmpeg-python

import os, re, json, time, shlex, subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple
from yt_dlp import YoutubeDL
from yt_dlp.utils import PostProcessingError, DownloadError
import shutil

# ========== 配置区 ==========
SAVE_DIR = r"E:/coin_works/project/video_storage"
ARCHIVE_FILE = os.path.join(SAVE_DIR, "downloaded.txt")
USE_YTDLP_CLI = True
YTDLP_EXE = r"yt-dlp"
FFMPEG_BIN_DIR = r"E:\ffmpeg-master-latest-win64-gpl\ffmpeg-master-latest-win64-gpl\bin"

SOURCES = [
    "https://www.youtube.com/@KoluniteVIP", "https://www.youtube.com/@tiabtc",
    "https://www.youtube.com/@junzhangbtc", "https://www.youtube.com/@Traderfengge",
    "https://www.youtube.com/@suozhangketang", "https://www.youtube.com/@BTCfeiyang",
    "https://www.youtube.com/@crypto_punks", "https://www.youtube.com/@blockchaindailynews",
    "https://www.youtube.com/@dacapitalscom", "https://www.youtube.com/@BTCCocoo",
]
MAX_PER_SOURCE = 2
PREFERRED_SUB_LANGS_FOR_DOWNLOAD = ["zh-Hans", "zh", "zh-CN", "zh-Hant", "en"]
SKIP_LIVE = True
SKIP_SHORTS = True
MIN_DURATION_SEC = 60 if SKIP_SHORTS else 0
KEYWORDS_REGEX = r"(BTC|比特币)"
SEARCH_KEYWORD = ""
SEARCH_RESULTS_LIMIT = 20
TIME_WINDOW_DAYS = 1
REQUIRE_SUBTITLES = True
REQUIRE_MANUAL_SUBS = False
ALLOWED_SUB_LANGS = []
USE_COOKIES = True
COOKIE_FILE = r"E:\chromeDownload\cookies.txt"
MANIFEST_PATH = os.path.join(SAVE_DIR, "manifest.jsonl")

# 是否在运行前清空本地存储（视频/字幕/manifest/归档）
CLEAN_BEFORE_RUN = True

# 项目根与抽帧输出目录
PROJECT_ROOT = Path(__file__).resolve().parent
FRAMES_OUTPUT_DIR = PROJECT_ROOT / "frames_output"

def now(): return datetime.now().strftime("%H:%M:%S")
def ensure_dir(path: str): os.makedirs(path, exist_ok=True)
def _inject_cookies(opts: Dict[str, Any]):
    if USE_COOKIES and COOKIE_FILE: opts["cookiefile"] = COOKIE_FILE

def _probe_opts(client_chain: List[str]) -> Dict[str, Any]:
    opts = {
        "extract_flat": "in_playlist", "playlistend": MAX_PER_SOURCE,
        "extractor_args": {"youtube": {"player_client": client_chain, "tab": ["videos"]}},
        "socket_timeout": 20, "extractor_retries": 6,
        "retry_sleep_functions": {"extractor": "exponential(1,2,5)"},
        "forceipv4": True, "quiet": True, "skip_download": True,
    }
    _inject_cookies(opts)
    return opts

def _video_probe_opts_base() -> Dict[str, Any]:
    opts = {
        "noplaylist": True, "socket_timeout": 20, "extractor_retries": 6,
        "retry_sleep_functions": {"extractor": "exponential(1,2,5)"},
        "forceipv4": True, "quiet": True, "skip_download": True,
    }
    _inject_cookies(opts)
    return opts

def _video_probe_opts_with_client(client_chain: List[str]) -> Dict[str, Any]:
    opts = _video_probe_opts_base()
    opts["extractor_args"] = {"youtube": {"player_client": client_chain}}
    return opts

def _extract_video_info_with_fallback(url: str) -> Optional[Dict[str, Any]]:
    try:
        with YoutubeDL(_video_probe_opts_base()) as ydl:
            info = ydl.extract_info(url, download=False)
        if (info.get("subtitles") or {}) or (info.get("automatic_captions") or {}):
            return info
    except Exception:
        info = None

    for chain in (["web"], ["android"], ["ios"], ["mweb"], ["tv"]):
        try:
            with YoutubeDL(_video_probe_opts_with_client(chain)) as ydl:
                info2 = ydl.extract_info(url, download=False)
            if (info2.get("subtitles") or {}) or (info2.get("automatic_captions") or {}):
                return info2
            info = info2
        except Exception:
            continue
    return info

def _client_chains_for_probe() -> List[List[str]]:
    if USE_COOKIES and COOKIE_FILE:
        return [["web"], ["web_embedded"], ["android"], ["ios"]]
    return [["android"], ["web"], ["web_embedded"]]

def choose_langs_for_download(info: Dict[str, Any]) -> List[str]:
    subs = set((info.get("subtitles") or {}).keys())
    autos = set((info.get("automatic_captions") or {}).keys())
    available = list(subs | autos)
    if not available: return []
    ordered = [l for l in PREFERRED_SUB_LANGS_FOR_DOWNLOAD if l in subs or l in autos]
    if ordered: return ordered
    def score(lang: str) -> int:
        base = 0
        if lang.startswith("zh"): base -= 10
        if lang.startswith("en"): base -= 5
        return base
    return sorted(available, key=score)

def fetch_channel_videos(handle_url: str) -> List[Dict[str, Any]]:
    base = handle_url.rstrip("/")
    if "/videos" not in base: base += "/videos"
    url = base + "?view=0&sort=dd"

    last_err = None
    for chain in _client_chains_for_probe():
        print(f"  [{now()}] 探测频道（client={chain}）…", flush=True)
        try:
            with YoutubeDL(_probe_opts(chain)) as ydl:
                info = ydl.extract_info(url, download=False)
                entries = info.get("entries") or []
                if entries:
                    print(f"  [{now()}] 取到 {len(entries)} 条（仅扁平元数据）", flush=True)
                    return entries
        except Exception as e:
            last_err = e
            continue
    raise last_err or RuntimeError("无法获取频道视频列表")

def pick_latest_urls_from_entries(entries: List[Dict[str, Any]], max_n: int) -> List[str]:
    def sort_key(e): return (e.get("upload_date") or "00000000", e.get("timestamp") or 0)
    entries = sorted(entries, key=sort_key, reverse=True)
    urls = []
    for e in entries:
        if SKIP_LIVE and (e.get("is_live") or e.get("was_live")): continue
        dur = e.get("duration") or 0
        if MIN_DURATION_SEC and dur and dur < MIN_DURATION_SEC: continue
        title = e.get("title") or ""
        if KEYWORDS_REGEX and not re.search(KEYWORDS_REGEX, title, flags=re.IGNORECASE): continue
        u = e.get("url") or e.get("webpage_url")
        if u:
            if len(u) == 11 and "/watch?" not in u: u = f"https://www.youtube.com/watch?v={u}"
            urls.append(u)
        if len(urls) >= max_n: break
    return urls

def probe_info(url: str) -> Dict[str, Any]:
    info = _extract_video_info_with_fallback(url)
    if not info: raise RuntimeError("无法获取视频信息")
    return info

def _expected_files_after_cli(info: Dict[str, Any]) -> List[Path]:
    filepaths = []
    title = info.get("title") or "video"
    uploader = info.get("uploader") or "unknown"
    up_date = info.get("upload_date") or ""
    if up_date and len(up_date) >= 8:
        up_date = f"{up_date[0:4]}-{up_date[4:6]}-{up_date[6:8]}"
    base = Path(SAVE_DIR) / uploader / f"{up_date} - {title}"
    for ext in (".mkv", ".mp4", ".webm", ".m4v"):
        cand = Path(str(base) + ext)
        if cand.exists(): filepaths.append(cand)
    return filepaths

def _pick_best_sub_for(video_path: Path, preferred: List[str]) -> Optional[Path]:
    base = video_path.with_suffix("")
    candidates = []
    for ext in (".srt", ".vtt", ".ass", ".ssa"):
        patterns = [base.name + f".*.{ext.lstrip('.')}", base.name + f"-*.{ext.lstrip('.')}", base.name + f"*.{ext.lstrip('.')}"]
        for pattern in patterns:
            candidates.extend(sorted(base.parent.glob(pattern)))
    if not candidates: return None

    def lang_of(p: Path) -> str:
        parts = p.name.split(".")
        if len(parts) >= 3: return parts[-2]
        name = p.stem
        if "." in name:
            lang_part = name.split(".")[-1]
            if len(lang_part) <= 5: return lang_part
        return ""

    for lang in preferred:
        for p in candidates:
            if lang_of(p) == lang: return p
    return candidates[0]

def append_manifest(record: dict):
    manifest = Path(MANIFEST_PATH)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    with manifest.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def _emit_subtitle_txt(video_path: str, subtitle_path: Optional[str]) -> None:
    try:
        if not subtitle_path: return
        video_file = Path(video_path)
        txt_path = video_file.with_suffix(".txt")
        txt_path.write_text(
            Path(subtitle_path).read_text(encoding="utf-8", errors="ignore"),
            encoding="utf-8", errors="ignore",
        )
    except Exception as e:
        print(f"  [warn] 生成字幕 txt 失败：{e}")

def _run_yt_dlp_cli(url: str, sub_lang: str = "zh-Hans", client: str = "web") -> None:
    cmd = [
        YTDLP_EXE, "--write-sub", "--write-auto-sub", "--sub-langs", sub_lang,
        "--convert-subs", "srt", "--paths", f"home:{SAVE_DIR}",
        "-o", "%(uploader)s/%(upload_date>%Y-%m-%d)s - %(title)s.%(ext)s",
        "--skip-unavailable-fragments", "--retries", "2", "--fragment-retries", "2", url,
    ]
    if ARCHIVE_FILE:
        cmd.insert(-1, "--download-archive")
        cmd.insert(-1, ARCHIVE_FILE)
    if USE_COOKIES and COOKIE_FILE:
        cmd.insert(-1, "--cookies")
        cmd.insert(-1, COOKIE_FILE)
    if FFMPEG_BIN_DIR:
        cmd.insert(-1, "--ffmpeg-location")
        cmd.insert(-1, FFMPEG_BIN_DIR)

    print("  [cli] 执行：", " ".join(shlex.quote(x) for x in cmd))
    subprocess.run(cmd, check=True)

def _search_opts() -> Dict[str, Any]:
    opts = {
        "quiet": True, "skip_download": True, "forceipv4": True,
        "extractor_retries": 4, "retry_sleep_functions": {"extractor": "exponential(1,2,5)"},
        "socket_timeout": 20,
    }
    _inject_cookies(opts)
    return opts

def search_videos_by_keyword(keyword: str, limit: int) -> List[Dict[str, Any]]:
    if not keyword: return []
    query = f"ytsearchdate{limit}:{keyword}"
    print(f"\n== 关键词搜索：{keyword}（取最新 {limit} 条）")
    results = []

    with YoutubeDL({**_search_opts(), "extract_flat": True}) as ydl:
        info = ydl.extract_info(query, download=False) or {}
        entries = info.get("entries") or []
    ids_or_urls = []
    for e in entries:
        u = e.get("url") or e.get("webpage_url") or e.get("id")
        if not u: continue
        if len(u) == 11 and "/watch?" not in u: u = f"https://www.youtube.com/watch?v={u}"
        ids_or_urls.append(u)

    for u in ids_or_urls:
        info_full = _extract_video_info_with_fallback(u)
        if info_full: results.append(info_full)
    print(f"  [{now()}] 搜索到 {len(results)} 条候选（已补全元数据）")
    return results

def _is_within_time_window(upload_date_str: Optional[str], days: int) -> bool:
    if not upload_date_str: return False
    try:
        up_date = datetime.strptime(upload_date_str, "%Y%m%d").date()
    except Exception:
        return False
    today_utc = datetime.now(timezone.utc).date()
    if days <= 0: return up_date == today_utc
    return (today_utc - up_date).days <= days

def _has_subtitles(info: Dict[str, Any]) -> Tuple[bool, List[str], bool]:
    subs = info.get("subtitles") or {}
    autos = info.get("automatic_captions") or {}

    if not REQUIRE_SUBTITLES: return True, [], False
    has_any = bool(subs) or bool(autos)
    if not has_any: return False, [], False

    if ALLOWED_SUB_LANGS:
        pool = set(ALLOWED_SUB_LANGS)
        subs_hit = [l for l in subs.keys() if l in pool]
        autos_hit = [l for l in autos.keys() if l in pool]
        if REQUIRE_MANUAL_SUBS:
            if subs_hit: return True, choose_langs_for_download({"subtitles": {k: None for k in subs_hit}, "automatic_captions": {}}), True
            return False, [], True
        else:
            hit = subs_hit or autos_hit
            if hit:
                ordered = [l for l in PREFERRED_SUB_LANGS_FOR_DOWNLOAD if l in hit]
                if ordered: return True, ordered, False
                return True, hit, False
            return False, [], False
    return True, [], REQUIRE_MANUAL_SUBS

def filter_search_results(infos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for info in infos:
        if SKIP_LIVE and (info.get("is_live") or info.get("was_live")): continue
        dur = info.get("duration") or 0
        if MIN_DURATION_SEC and dur and dur < MIN_DURATION_SEC: continue
        if not _is_within_time_window(info.get("upload_date"), TIME_WINDOW_DAYS): continue
        ok, langs, manual_only = _has_subtitles(info)
        if not ok: continue
        info["_preferred_sub_langs"] = langs
        info["_need_manual_only"] = manual_only
        out.append(info)
    out.sort(key=lambda e: (e.get("upload_date") or "00000000", e.get("timestamp") or 0), reverse=True)
    return out

def filter_channel_targets_by_subtitles(urls: List[str]) -> List[Tuple[str, Optional[List[str]]]]:
    out = []
    for u in urls:
        try:
            info = probe_info(u)
        except Exception:
            continue
        ok, langs, manual_only = _has_subtitles(info)
        if not ok: continue
        if not langs: langs = choose_langs_for_download(info) or []
        show_langs = langs if langs else PREFERRED_SUB_LANGS_FOR_DOWNLOAD
        print(f"  -> 命中：{info.get('upload_date')} | {info.get('title')} | 计划请求字幕: {show_langs}")
        out.append((info.get("webpage_url") or u, langs if langs else None))
    return out

def _normalize_author_name(name: str) -> str:
    # 去掉所有空白（含 NBSP）、非法文件字符
    s = name.replace("\u00A0", "")
    s = re.sub(r"\s+", "", s)
    s = re.sub(r'[<>:"/\\|?*]', '', s)
    return s or "unknown"

def _next_seq_for(uploader_dir: Path, date_str: str, author_norm: str) -> int:
    # 计算该作者目录下、同日的下一个编号
    pat_prefix = f"{date_str}-{author_norm}-"
    max_n = 0
    for p in uploader_dir.iterdir():
        if not p.is_file():
            continue
        stem = p.stem  # 不含扩展名
        m = re.match(rf'^{re.escape(pat_prefix)}(\d+)$', stem)
        if m:
            n = int(m.group(1))
            if n > max_n:
                max_n = n
    return max_n + 1

def _rename_video_and_emit_txt(vp: Path, info: Dict[str, Any], subtitle_path: Optional[Path]) -> Path:
    """
    将下载好的视频 vp 重命名为 YYYY-MM-DD-作者-编号.ext，
    并根据 subtitle_path 生成同名 .txt 字幕。
    返回新的视频路径。
    """
    uploader_dir = vp.parent
    up_date = info.get("upload_date") or ""
    date_str = f"{up_date[0:4]}-{up_date[4:6]}-{up_date[6:8]}" if len(up_date) >= 8 else "0000-00-00"
    author_norm = _normalize_author_name(info.get("uploader") or uploader_dir.name)

    idx = _next_seq_for(uploader_dir, date_str, author_norm)
    new_stem = f"{date_str}-{author_norm}-{idx}"
    # 按示例，视频扩展名可保持原始大小写；如需全大写可用 vp.suffix.upper()
    new_video_path = uploader_dir / (new_stem + vp.suffix)

    # 若意外冲突，则递增编号
    while new_video_path.exists():
        idx += 1
        new_stem = f"{date_str}-{author_norm}-{idx}"
        new_video_path = uploader_dir / (new_stem + vp.suffix)

    # 执行重命名
    try:
        vp.rename(new_video_path)
    except Exception as e:
        print(f"  [warn] 重命名失败，保留原名：{vp.name} -> {new_video_path.name} | {e}")
        new_video_path = vp  # 失败则退回原路径

    # 生成同名 .txt
    try:
        if subtitle_path and Path(subtitle_path).exists():
            txt_path = new_video_path.with_suffix(".txt")
            txt_path.write_text(Path(subtitle_path).read_text(encoding="utf-8", errors="ignore"),
                                encoding="utf-8", errors="ignore")
    except Exception as e:
        print(f"  [warn] 生成字幕 txt 失败：{e}")

    return new_video_path

def download_one(url: str, preferred_langs_from_search: Optional[List[str]] = None):
    try:
        info = probe_info(url)
    except Exception as e:
        print(f"  [warn] 获取视频信息失败：{e}")
        return

    if preferred_langs_from_search and len(preferred_langs_from_search) > 0:
        expected_langs = preferred_langs_from_search
    else:
        expected_langs = choose_langs_for_download(info) or PREFERRED_SUB_LANGS_FOR_DOWNLOAD

    if not expected_langs:
        print(f"  [warn] 未检测到字幕，尝试下载所有可用字幕")
        expected_langs = ["all"]

    print(f"  [{now()}] 开始下载：{url}")

    if USE_YTDLP_CLI:
        last_err = None
        for client in ("web", "android", "ios"):
            try:
                sub_lang = "all" if expected_langs == ["all"] else (expected_langs[0] if expected_langs else "zh-Hans")
                _run_yt_dlp_cli(url, sub_lang=sub_lang, client=client)
                last_err = None
                break
            except subprocess.CalledProcessError as e:
                last_err = e
                print(f"  [warn] CLI 下载失败（client={client}）：{e}")
                continue

        if last_err:
            print("  [error] 所有 CLI 客户端均失败，放弃该视频。")
            return

        video_files = _expected_files_after_cli(info)
        if not video_files:
            print("  [warn] CLI 后未能定位输出文件。")
            return

        for vp in video_files:
            # 尝试找到最佳字幕文件
            best_sub = _pick_best_sub_for(vp, expected_langs if expected_langs != ["all"] else PREFERRED_SUB_LANGS_FOR_DOWNLOAD)
            if not best_sub and expected_langs == ["all"]:
                best_sub = _pick_best_sub_for(vp, [])

            # 先重命名视频并生成同名 .txt
            new_vp = _rename_video_and_emit_txt(vp, info, best_sub)

            record = {
                "id": info.get("id"),
                "title": info.get("title"),
                "uploader": info.get("uploader"),
                "channel_id": info.get("channel_id"),
                "upload_date": info.get("upload_date"),
                "url": info.get("webpage_url") or url,
                "video_path": str(new_vp.resolve()),
                "subtitle_path": str(best_sub.resolve()) if best_sub else None,
                "created_at": int(time.time()),
            }
            append_manifest(record)
            print(f"  [manifest] 已记录：{new_vp.name} -> {best_sub.name if best_sub else '（无字幕）'}")
        return

    # API 路径（保留）
    result = None
    last_err = None
    for client_chain in (["web"], ["android"], ["ios"]):
        try:
            opts = _download_opts(expected_langs, client_chain)
            with YoutubeDL(opts) as ydl:
                result = ydl.extract_info(url, download=True)
            last_err = None
            break
        except (DownloadError, PostProcessingError) as e:
            last_err = e
            print(f"  [warn] 下载阶段失败/转码失败（client={client_chain}）：{e}")
            continue
        except Exception as e:
            last_err = e
            print(f"  [warn] 下载阶段异常（client={client_chain}）：{e}")
            continue

    if last_err:
        print("  [error] 所有下载客户端均失败，放弃该视频。")
        return

    video_files = _expected_files_after_cli(result) if isinstance(result, dict) else _expected_files_after_cli(info)
    if not video_files:
        print("  [warn] 未能定位视频文件路径。")
        return

    for vp in video_files:
        best_sub = _pick_best_sub_for(vp, expected_langs)

        # 先重命名视频并生成同名 .txt
        new_vp = _rename_video_and_emit_txt(vp, (result if isinstance(result, dict) else info), best_sub)

        record = {
            "id": result.get("id") if isinstance(result, dict) else info.get("id"),
            "title": result.get("title") if isinstance(result, dict) else info.get("title"),
            "uploader": result.get("uploader") if isinstance(result, dict) else info.get("uploader"),
            "channel_id": result.get("channel_id") if isinstance(result, dict) else info.get("channel_id"),
            "upload_date": result.get("upload_date") if isinstance(result, dict) else info.get("upload_date"),
            "url": (result.get("webpage_url") if isinstance(result, dict) else info.get("webpage_url")) or url,
            "video_path": str(new_vp.resolve()),
            "subtitle_path": str(best_sub.resolve()) if best_sub else None,
            "created_at": int(time.time()),
        }
        append_manifest(record)
        print(f"  [manifest] 已记录：{new_vp.name} -> {best_sub.name if best_sub else '（无字幕）'}")

def _download_opts(sub_langs: Optional[List[str]], client_chain: List[str]) -> Dict[str, Any]:
    ensure_dir(SAVE_DIR)
    langs = sub_langs if (sub_langs and len(sub_langs) > 0) else PREFERRED_SUB_LANGS_FOR_DOWNLOAD
    opts = {
        "paths": {"home": SAVE_DIR}, "outtmpl": "%(uploader)s/%(upload_date>%Y-%m-%d)s - %(title)s.%(ext)s",
        "nooverwrites": True, "noplaylist": True, "writesubtitles": True, "writeautomaticsub": True,
        "subtitlesformat": "vtt", "subtitleslangs": langs[:1],
        "postprocessors": [
            {"key": "FFmpegSubtitlesConvertor", "format": "srt"},
            {"key": "FFmpegVideoRemuxer", "preferedformat": "mkv"},
        ],
        "extractor_args": {"youtube": {"player_client": client_chain}},
        "socket_timeout": 30, "extractor_retries": 6, "retry_sleep_functions": {"extractor": "exponential(1,2,5)"},
        "forceipv4": True, "check_formats": True, "merge_output_format": "mkv",
        "ffmpeg_location": FFMPEG_BIN_DIR or None,
        #"format": "bv*+ba/b" if not LIMIT_1080P else "bv*[height<=1080]+ba/b[height<=1080]",
    }
    return opts

def _clean_storage():
    root = Path(SAVE_DIR)
    if root.exists():
        for child in root.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink()
            except Exception as e:
                print(f"  [warn] 清理失败：{child} | {e}")
    # 额外：清理抽帧输出目录
    try:
        if FRAMES_OUTPUT_DIR.exists():
            shutil.rmtree(FRAMES_OUTPUT_DIR, ignore_errors=True)
    except Exception as e:
        print(f"  [warn] 清理失败：{FRAMES_OUTPUT_DIR} | {e}")

def main():
    ensure_dir(SAVE_DIR)
    if ARCHIVE_FILE: ensure_dir(os.path.dirname(ARCHIVE_FILE))

    if CLEAN_BEFORE_RUN:
        print("[clean] 清空本地视频/字幕/清单…")
        _clean_storage()

    print(f"[{datetime.now().strftime('%F %T')}] 开始任务")
    print(f"保存目录：{SAVE_DIR}")
    if USE_COOKIES and COOKIE_FILE: print(f"使用 cookies 文件：{COOKIE_FILE}")

    all_targets = []

    if SEARCH_KEYWORD.strip():
        print(f"\n[搜索模式] 关键词：{SEARCH_KEYWORD} | 时间窗口：{TIME_WINDOW_DAYS} 天 | 只要检测到字幕就下载：{REQUIRE_SUBTITLES}")
        raw_infos = search_videos_by_keyword(SEARCH_KEYWORD.strip(), SEARCH_RESULTS_LIMIT)

        print("\n[诊断] 搜索候选概览（日期/时长/字幕语言）")
        for i, info in enumerate(raw_infos, 1):
            up = info.get("upload_date")
            dur = info.get("duration")
            subs = sorted((info.get("subtitles") or {}).keys())
            autos = sorted((info.get("automatic_captions") or {}).keys())
            print(f"  {i:02d}. {up} | {dur or '-'}s | subs={subs[:6]}{'...' if len(subs)>6 else ''} | autos={autos[:6]}{'...' if len(autos)>6 else ''}")

        filtered = filter_search_results(raw_infos)
        if not filtered: print("（没有满足条件的搜索结果）")
        for info in filtered:
            url = info.get("webpage_url") or f"https://www.youtube.com/watch?v={info.get('id')}"
            langs = info.get("_preferred_sub_langs") or []
            show_langs = langs if langs else PREFERRED_SUB_LANGS_FOR_DOWNLOAD
            print(f"  -> 命中：{info.get('upload_date')} | {info.get('title')} | 计划请求字幕: {show_langs}")
            all_targets.append((url, langs if langs else None))
    else:
        if KEYWORDS_REGEX: print(f"关键词过滤（频道模式）：/{KEYWORDS_REGEX}/  （大小写不敏感）")
        if SKIP_SHORTS: print("跳过 Shorts（duration < 60s）已启用。")
        if SKIP_LIVE: print("跳过直播/回放已启用。")

        urls = []
        for src in SOURCES:
            print(f"\n== 解析来源：{src}")
            try:
                entries = fetch_channel_videos(src)
            except Exception as e:
                print(f"  [warn] 获取频道视频失败：{e}")
                continue
            picked = pick_latest_urls_from_entries(entries, MAX_PER_SOURCE)
            if not picked:
                print("  （筛选后无匹配的视频）")
                continue
            for u in picked: print("  -> 目标视频：", u)
            urls.extend(picked)

        if REQUIRE_SUBTITLES or REQUIRE_MANUAL_SUBS or (ALLOWED_SUB_LANGS and len(ALLOWED_SUB_LANGS) > 0):
            filtered_targets = filter_channel_targets_by_subtitles(urls)
            if not filtered_targets: print("（频道模式：无满足字幕条件的视频）")
            all_targets = filtered_targets
        else:
            all_targets = [(u, None) for u in urls]

    if not all_targets:
        print("\n没有可下载的视频。完成 ✅")
        return

    print(f"\n共需下载 {len(all_targets)} 个视频。开始下载…")
    for u, langs in all_targets:
        download_one(u, preferred_langs_from_search=langs)

    print("\n全部完成 ✅")

if __name__ == "__main__":
    main()
