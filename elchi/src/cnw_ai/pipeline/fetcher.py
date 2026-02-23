"""Fetch strategies: git, web, local, jsonl."""

from __future__ import annotations

import fnmatch
from pathlib import Path

import git as gitpython

from cnw_ai.config import WORKDIR
from cnw_ai.pipeline.models import SourceConfig
from cnw_ai.utils.logging import get_logger

log = get_logger(__name__)


def _match_globs(path: Path, root: Path, globs: list[str]) -> bool:
    """Check if a path matches any of the glob patterns."""
    rel = str(path.relative_to(root))
    return any(fnmatch.fnmatch(rel, g) for g in globs)


def _collect_files(
    root: Path, include_globs: list[str], exclude_globs: list[str], max_items: int = 0
) -> list[Path]:
    """Collect files matching include globs, excluding exclude globs."""
    files: list[Path] = []

    if not include_globs:
        # Default: all files
        include_globs = ["**/*"]

    for pattern in include_globs:
        for path in root.glob(pattern):
            if not path.is_file():
                continue
            if exclude_globs and _match_globs(path, root, exclude_globs):
                continue
            files.append(path)

    # Deduplicate and sort
    files = sorted(set(files))

    if max_items > 0:
        files = files[:max_items]

    return files


def fetch_git(source: SourceConfig, max_items: int = 0) -> list[Path]:
    """Clone or pull a git repository, then collect matching files."""
    workdir = WORKDIR / source.id
    workdir.parent.mkdir(parents=True, exist_ok=True)

    if workdir.exists() and (workdir / ".git").exists():
        log.info("git_pull", source_id=source.id, path=str(workdir))
        try:
            repo = gitpython.Repo(workdir)
            repo.remotes.origin.pull()
        except Exception as e:
            log.warning("git_pull_failed", source_id=source.id, error=str(e))
    else:
        log.info("git_clone", source_id=source.id, url=source.location, branch=source.branch)
        gitpython.Repo.clone_from(
            source.location,
            str(workdir),
            branch=source.branch,
            depth=1,
        )

    # Set version from branch
    if not source.version:
        source.version = source.branch

    return _collect_files(workdir, source.include_globs, source.exclude_globs, max_items)


def fetch_web(source: SourceConfig, max_items: int = 0) -> list[Path]:
    """Download web pages with httpx, cache in workdir."""
    import httpx

    workdir = WORKDIR / source.id
    workdir.mkdir(parents=True, exist_ok=True)

    urls = [source.location]
    downloaded: list[Path] = []

    for url in urls:
        # Simple filename from URL
        safe_name = url.split("//")[-1].replace("/", "_").replace("?", "_")[:200]
        if not safe_name.endswith(".html"):
            safe_name += ".html"
        dest = workdir / safe_name

        if dest.exists():
            log.debug("web_cached", url=url, path=str(dest))
            downloaded.append(dest)
            continue

        log.info("web_download", url=url)
        try:
            resp = httpx.get(url, follow_redirects=True, timeout=30)
            resp.raise_for_status()
            dest.write_text(resp.text, encoding="utf-8")
            downloaded.append(dest)
        except Exception as e:
            log.warning("web_download_failed", url=url, error=str(e))

        if max_items > 0 and len(downloaded) >= max_items:
            break

    return downloaded


def fetch_local(source: SourceConfig, max_items: int = 0) -> list[Path]:
    """Collect files from a local directory."""
    root = Path(source.location)
    if not root.exists():
        log.warning("local_not_found", source_id=source.id, path=str(root))
        return []

    return _collect_files(root, source.include_globs, source.exclude_globs, max_items)


def fetch_jsonl(source: SourceConfig, max_items: int = 0) -> list[Path]:
    """Return the JSONL file path directly."""
    path = Path(source.location)
    if not path.is_absolute():
        from cnw_ai.config import PROJECT_ROOT
        path = PROJECT_ROOT / path

    if not path.exists():
        log.warning("jsonl_not_found", source_id=source.id, path=str(path))
        return []

    return [path]


# Strategy mapping
FETCH_STRATEGIES = {
    "git": fetch_git,
    "web": fetch_web,
    "local": fetch_local,
    "jsonl": fetch_jsonl,
}


def fetch(source: SourceConfig, max_items: int = 0) -> list[Path]:
    """Fetch files for a source using the appropriate strategy."""
    strategy = FETCH_STRATEGIES.get(source.source_type)
    if strategy is None:
        log.error("unknown_source_type", source_type=source.source_type)
        return []

    files = strategy(source, max_items)
    log.info("fetched", source_id=source.id, file_count=len(files))
    return files
