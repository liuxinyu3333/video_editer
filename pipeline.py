import json
import time
import shutil
from pathlib import Path
from typing import List, Dict, Any
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import video_loader
import video_cut


def _read_new_records(manifest_path: Path, since_ts: int) -> List[Dict[str, Any]]:
    if not manifest_path.exists():
        return []
    records: List[Dict[str, Any]] = []
    with manifest_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            if int(rec.get("created_at", 0)) >= since_ts:
                records.append(rec)
    return records


def _zip_frames_dir(frames_dir: Path, zip_path: Path) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    # shutil.make_archive expects base_name without extension
    base = zip_path.with_suffix("")
    # Remove previous archive if exists
    for ext in (".zip",):
        p = base.with_suffix(ext)
        if p.exists():
            p.unlink()
    shutil.make_archive(str(base), "zip", root_dir=str(frames_dir))


def _prepare_video_folder_and_move_txt(video_path: Path) -> Path:
    """Create a per-video folder under the uploader directory and move the subtitle txt into it.
    Returns the created folder path.
    """
    video_dir = video_path.parent
    video_base = video_path.stem
    target_dir = video_dir / video_base
    target_dir.mkdir(parents=True, exist_ok=True)

    txt_path = video_path.with_suffix(".txt")
    if txt_path.exists():
        target_txt = target_dir / txt_path.name
        if target_txt.exists():
            target_txt.unlink()
        shutil.move(str(txt_path), str(target_txt))
    return target_dir


def main():
    start_ts = int(time.time())

    # 1) 下载视频与字幕（依赖 video_loader 的配置与筛选）
    video_loader.main()

    manifest_path = Path(video_loader.MANIFEST_PATH)

    # 2) 读取本次新增记录
    new_records = _read_new_records(manifest_path, since_ts=start_ts)
    if not new_records:
        print("[pipeline] 本次无新增下载记录，结束。")
        return

    # 3) 抽帧（按字幕起始/中点/结束，并进行相似度过滤）
    out_root = Path(video_cut.DEFAULT_OUTPUT_DIR)
    out_root.mkdir(parents=True, exist_ok=True)

    for rec in new_records:
        v = rec.get("video_path")
        s = rec.get("subtitle_path")
        if not v or not s:
            continue
        vpath = Path(v)
        spath = Path(s)
        if not vpath.exists() or not spath.exists():
            continue

        # 抽帧
        video_cut.process_one_video(vpath, spath, out_root, max_subs=0, similarity_threshold=5)

        # 找到该视频对应的帧目录（与 video_cut 使用相同规则）
        frames_dir = video_cut._choose_out_dir(out_root, uploader=vpath.parent.name, base=vpath.stem)
        if not frames_dir.exists():
            # 若未找到，跳过打包
            print(f"[pipeline] 未找到帧目录：{frames_dir}")
            continue

        # 4) 在视频专属目录内，仅保留 zip 与字幕 txt
        video_folder = _prepare_video_folder_and_move_txt(vpath)
        zip_path = video_folder / "frames.zip"
        _zip_frames_dir(frames_dir, zip_path)

        # 清理：确保专属目录仅包含 zip 与 txt
        for child in video_folder.iterdir():
            if child.name.lower().endswith(".zip"):
                continue
            if child.name.lower().endswith(".txt"):
                continue
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                try:
                    child.unlink()
                except Exception:
                    pass

        print(f"[pipeline] 完成：{vpath.stem} -> {zip_path}")


if __name__ == "__main__":


    try:


        TZ = ZoneInfo("Asia/Shanghai")
    except Exception:
        TZ = None  # 没有 zoneinfo 就用本地时区

    while True:
        now = datetime.now(TZ) if TZ else datetime.now()
        nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        time.sleep((nxt - now).total_seconds())
        try:
            main()  # 每天 00:00 执行一次
        except Exception:
            import traceback;

            traceback.print_exc()
        time.sleep(2)  # 防抖，避免误触发


