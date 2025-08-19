#!/usr/bin/env python3
"""
字幕下载诊断工具
用于分析视频字幕下载失败的原因
"""

import json
import subprocess
import shlex
from pathlib import Path
from typing import Dict, Any, List, Optional

from yt_dlp import YoutubeDL


def probe_video_subtitles(url: str) -> Dict[str, Any]:
    """探测视频的字幕信息"""
    print(f"\n=== 探测视频字幕信息 ===")
    print(f"URL: {url}")
    
    opts = {
        "quiet": True,
        "skip_download": True,
        "forceipv4": True,
        "extractor_retries": 3,
        "socket_timeout": 20,
    }
    
    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
            
        print(f"视频标题: {info.get('title', 'Unknown')}")
        print(f"上传者: {info.get('uploader', 'Unknown')}")
        print(f"视频ID: {info.get('id', 'Unknown')}")
        
        # 分析字幕信息
        subtitles = info.get("subtitles", {})
        automatic_captions = info.get("automatic_captions", {})
        
        print(f"\n手动字幕 (subtitles):")
        if subtitles:
            for lang, formats in subtitles.items():
                print(f"  {lang}: {len(formats)} 个格式")
                for fmt in formats[:3]:  # 只显示前3个格式
                    print(f"    - {fmt.get('ext', 'unknown')} ({fmt.get('name', 'unknown')})")
        else:
            print("  无手动字幕")
            
        print(f"\n自动字幕 (automatic_captions):")
        if automatic_captions:
            for lang, formats in automatic_captions.items():
                print(f"  {lang}: {len(formats)} 个格式")
                for fmt in formats[:3]:  # 只显示前3个格式
                    print(f"    - {fmt.get('ext', 'unknown')} ({fmt.get('name', 'unknown')})")
        else:
            print("  无自动字幕")
            
        return info
        
    except Exception as e:
        print(f"探测失败: {e}")
        return {}


def test_yt_dlp_subtitle_download(url: str, lang: str = "zh-Hans") -> bool:
    """测试 yt-dlp 字幕下载功能"""
    print(f"\n=== 测试字幕下载 ===")
    print(f"目标语言: {lang}")
    
    cmd = [
        "yt-dlp",
        "--write-sub",
        "--write-auto-sub", 
        "--sub-langs", lang,
        "--convert-subs", "srt",
        "--skip-download",  # 只下载字幕，不下载视频
        "--output", "test_subtitle_%(id)s.%(ext)s",
        url
    ]
    
    print(f"执行命令: {' '.join(shlex.quote(x) for x in cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✅ 字幕下载测试成功")
            print("输出:")
            print(result.stdout)
            return True
        else:
            print("❌ 字幕下载测试失败")
            print("错误输出:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ 字幕下载测试超时")
        return False
    except Exception as e:
        print(f"❌ 字幕下载测试异常: {e}")
        return False


def analyze_manifest(manifest_path: str) -> None:
    """分析 manifest.jsonl 文件中的字幕记录"""
    print(f"\n=== 分析 Manifest 文件 ===")
    manifest_file = Path(manifest_path)
    
    if not manifest_file.exists():
        print(f"Manifest 文件不存在: {manifest_path}")
        return
        
    records = []
    with manifest_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except:
                    continue
    
    print(f"总记录数: {len(records)}")
    
    # 统计字幕情况
    with_subtitle = 0
    without_subtitle = 0
    
    for rec in records:
        if rec.get("subtitle_path"):
            with_subtitle += 1
        else:
            without_subtitle += 1
            
    print(f"有字幕: {with_subtitle}")
    print(f"无字幕: {without_subtitle}")
    print(f"字幕成功率: {with_subtitle/(with_subtitle+without_subtitle)*100:.1f}%")
    
    # 显示最近几条无字幕的记录
    print(f"\n最近无字幕的记录:")
    count = 0
    for rec in reversed(records):
        if not rec.get("subtitle_path") and count < 5:
            print(f"  {rec.get('title', 'Unknown')} (ID: {rec.get('id', 'Unknown')})")
            count += 1


def check_subtitle_files(video_storage_dir: str) -> None:
    """检查视频存储目录中的字幕文件"""
    print(f"\n=== 检查字幕文件 ===")
    storage_dir = Path(video_storage_dir)
    
    if not storage_dir.exists():
        print(f"存储目录不存在: {video_storage_dir}")
        return
    
    video_files = []
    subtitle_files = []
    
    # 递归查找所有视频和字幕文件
    for file_path in storage_dir.rglob("*"):
        if file_path.is_file():
            if file_path.suffix.lower() in (".mp4", ".mkv", ".webm", ".m4v"):
                video_files.append(file_path)
            elif file_path.suffix.lower() in (".srt", ".vtt", ".ass", ".ssa"):
                subtitle_files.append(file_path)
    
    print(f"视频文件数: {len(video_files)}")
    print(f"字幕文件数: {len(subtitle_files)}")
    
    # 检查每个视频文件是否有对应的字幕
    videos_with_subs = 0
    videos_without_subs = 0
    
    for video_file in video_files:
        base_name = video_file.stem
        has_subtitle = False
        
        for sub_file in subtitle_files:
            if sub_file.stem.startswith(base_name):
                has_subtitle = True
                break
                
        if has_subtitle:
            videos_with_subs += 1
        else:
            videos_without_subs += 1
            print(f"  无字幕视频: {video_file.name}")
    
    print(f"\n有字幕的视频: {videos_with_subs}")
    print(f"无字幕的视频: {videos_without_subs}")
    if videos_with_subs + videos_without_subs > 0:
        print(f"字幕覆盖率: {videos_with_subs/(videos_with_subs+videos_without_subs)*100:.1f}%")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="字幕下载诊断工具")
    parser.add_argument("--url", type=str, help="要诊断的YouTube视频URL")
    parser.add_argument("--manifest", type=str, default="video_storage/manifest.jsonl", help="manifest文件路径")
    parser.add_argument("--storage", type=str, default="video_storage", help="视频存储目录")
    parser.add_argument("--test-download", action="store_true", help="测试字幕下载功能")
    
    args = parser.parse_args()
    
    if args.url:
        # 探测指定视频的字幕信息
        info = probe_video_subtitles(args.url)
        
        if args.test_download:
            # 测试字幕下载
            test_yt_dlp_subtitle_download(args.url)
    
    # 分析现有数据
    analyze_manifest(args.manifest)
    check_subtitle_files(args.storage)
    
    print(f"\n=== 诊断建议 ===")
    print("1. 检查网络连接和YouTube访问是否正常")
    print("2. 确认 yt-dlp 版本是否最新: pip install -U yt-dlp")
    print("3. 检查 cookies 文件是否有效")
    print("4. 某些视频可能确实没有字幕")
    print("5. 尝试使用不同的客户端 (web/android/ios)")
    print("6. 检查视频是否被限制访问")


if __name__ == "__main__":
    main()
