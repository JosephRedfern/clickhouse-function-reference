import csv
import json
import os
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from jinja2 import Environment, FileSystemLoader, select_autoescape
from loguru import logger

from scrape_docs import (
    get_anchor_function_doc_pages,
    get_direct_function_doc_urls,
    get_direct_statement_doc_urls,
    get_function_doc_pages,
    get_statement_doc_pages,
)

ALLOWED_TAGS = {"latest", "head"}
MUTABLE_TAGS = {"latest", "head"}
USE_FIDDLE = False
WORKERS = 8  # concurrent Docker containers; ignored when USE_FIDDLE=True
DOCS_FETCH_WORKERS = 16  # concurrent ClickHouse docs page fetches for URL discovery
CONTAINER_NAME_TEMPLATE = "clickhouse-function-reference-{tag}"
CACHE_DIR = "cache"
IMAGE_DIGESTS_CACHE_PATH = os.path.join(CACHE_DIR, "image_digests.json")
FUNCTION_DOCS_CACHE_PATH = os.path.join(CACHE_DIR, "function_docs_urls.json")
KEYWORD_DOCS_CACHE_PATH = os.path.join(CACHE_DIR, "keyword_docs_urls.json")
SETTING_DOCS_CACHE_PATH = os.path.join(CACHE_DIR, "setting_docs_urls.json")
SETTINGS_DOCS_PAGE_URL = "https://clickhouse.com/docs/operations/settings/settings"
CURATED_DOCS_DIR = "curated_docs_urls"
CURATED_DOCS_PATHS = {
    "function": os.path.join(CURATED_DOCS_DIR, "functions.json"),
    "keyword": os.path.join(CURATED_DOCS_DIR, "keywords.json"),
    "setting": os.path.join(CURATED_DOCS_DIR, "settings.json"),
}
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
ASSETS_DIR = BASE_DIR / "assets"
DATA_DIR = ASSETS_DIR / "data"
template_env = Environment(
    loader=FileSystemLoader(TEMPLATES_DIR),
    autoescape=select_autoescape(["html", "xml"]),
)


def _process_version(
    version: str, force_refresh: bool = False
) -> tuple[str, list, list, list]:
    return (
        version,
        get_functions(version, force_refresh=force_refresh),
        get_keywords(version, force_refresh=force_refresh),
        get_settings(version, force_refresh=force_refresh),
    )


def main() -> None:
    tags = get_tags()
    versions = []
    for v in tags:
        versions.append(v)
        if v == "21.9":
            break

    image_digests = load_image_digests()
    resolved_digests = resolve_image_digests(versions)
    refresh_tags = {
        tag
        for tag in versions
        if tag in MUTABLE_TAGS
        and (
            not has_cached_data(tag)
            or (
                (digest := resolved_digests.get(tag)) is not None
                and image_digests.get(tag) != digest
            )
        )
    }

    workers = 1 if USE_FIDDLE else WORKERS
    results: dict[str, tuple] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_version, v, force_refresh=v in refresh_tags): v
            for v in versions
        }
        for future in as_completed(futures):
            version, funcs, keywords, settings = future.result()
            results[version] = (funcs, keywords, settings)
            logger.info(f"Finished {version}")
            if not USE_FIDDLE:
                _cleanup_container(version)

    for tag in refresh_tags:
        digest = resolved_digests.get(tag)
        if digest:
            image_digests[tag] = digest
    save_image_digests(image_digests)

    # Rebuild in original tag order so HTML columns stay consistent
    function_info = {}
    keyword_info = {}
    setting_info = {}
    for v in versions:
        function_info[v], keyword_info[v], setting_info[v] = results[v]

    render(
        function_info,
        title="ClickHouse Function Reference",
        header="ClickHouse Function Availability Reference",
        feature_type="function",
        filename="index.html",
    )
    render(
        keyword_info,
        title="ClickHouse Keyword Reference",
        header="ClickHouse Keyword Reference",
        feature_type="keyword",
        filename="keywords.html",
    )
    render(
        setting_info,
        title="ClickHouse Setting Reference",
        header="ClickHouse Setting Reference",
        feature_type="setting",
        filename="settings.html",
    )


def _cleanup_container(version: str) -> None:
    proc = subprocess.run(
        [
            "docker",
            "inspect",
            "-f",
            "{{.State.Running}}",
            CONTAINER_NAME_TEMPLATE.format(tag=version),
        ],
        capture_output=True,
    )
    if proc.returncode == 0 and proc.stdout.decode().strip() == "true":
        logger.info(f"Stopping container for {version}")
        subprocess.run(
            ["docker", "stop", CONTAINER_NAME_TEMPLATE.format(tag=version)],
            capture_output=True,
        )
        subprocess.run(
            ["docker", "rm", CONTAINER_NAME_TEMPLATE.format(tag=version)],
            capture_output=True,
        )


def load_json_cache(path: str):
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def save_json_cache(path: str, data) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def _normalize_feature_name(value: str) -> str:
    return re.sub(r"[\s_\-/]+", "", value).lower()


def _fetch_page_ids(page_url: str) -> tuple[str, list[str]]:
    logger.info(f"Fetching docs page: {page_url}")
    response = requests.get(page_url, timeout=30)
    response.raise_for_status()
    content = response.text
    matches = re.findall(r'\bid=(?:"([^"]+)"|([^\s>]+))', content)
    return page_url, [quoted_id or unquoted_id for quoted_id, unquoted_id in matches]


def _load_curated_docs_urls(feature_type: str) -> dict[str, str | None]:
    curated_path = CURATED_DOCS_PATHS[feature_type]
    curated = load_json_cache(curated_path)
    if not isinstance(curated, dict):
        return {}

    result: dict[str, str | None] = {}
    for key, value in curated.items():
        if not isinstance(key, str):
            continue
        if value is None or isinstance(value, str):
            result[key] = value
    return result


def _apply_curated_overrides(urls: dict[str, str], feature_type: str) -> dict[str, str]:
    curated = _load_curated_docs_urls(feature_type)
    if not curated:
        return urls

    merged = dict(urls)
    for feature, override_url in curated.items():
        if override_url is None:
            merged.pop(feature, None)
        else:
            merged[feature] = override_url
    return merged


def get_functions(tag: str, force_refresh: bool = False) -> list[dict]:
    cache_path = os.path.join(CACHE_DIR, "functions", f"{tag}.json")
    if not force_refresh:
        cached = load_json_cache(cache_path)
        if cached is not None:
            logger.info(f"Loaded cached functions for {tag}")
            return cached

    tsv = run_query("SELECT * FROM system.functions FORMAT TabSeparatedWithNames", tag)
    if tsv is None:
        return []
    result = list(csv.DictReader(tsv.splitlines(), delimiter="\t"))
    save_json_cache(cache_path, result)
    return result


def get_keywords(tag: str, force_refresh: bool = False) -> list[dict]:
    cache_path = os.path.join(CACHE_DIR, "keywords", f"{tag}.json")
    if not force_refresh:
        cached = load_json_cache(cache_path)
        if cached is not None:
            logger.info(f"Loaded cached keywords for {tag}")
            return cached

    tsv = run_query(
        "SELECT keyword as name FROM system.keywords FORMAT TabSeparatedWithNames", tag
    )
    if tsv is None:
        return []
    result = list(csv.DictReader(tsv.splitlines(), delimiter="\t"))
    save_json_cache(cache_path, result)
    return result


def get_settings(tag: str, force_refresh: bool = False) -> list[dict]:
    cache_path = os.path.join(CACHE_DIR, "settings", f"{tag}.json")
    if not force_refresh:
        cached = load_json_cache(cache_path)
        if cached is not None:
            logger.info(f"Loaded cached settings for {tag}")
            return cached

    tsv = run_query(
        "SELECT name, alias_for FROM system.settings FORMAT TabSeparatedWithNames", tag
    )
    if tsv is None:
        return []
    result = list(csv.DictReader(tsv.splitlines(), delimiter="\t"))
    save_json_cache(cache_path, result)
    return result


def load_image_digests() -> dict[str, str]:
    cached = load_json_cache(IMAGE_DIGESTS_CACHE_PATH)
    if isinstance(cached, dict):
        return {
            tag: digest for tag, digest in cached.items() if isinstance(digest, str)
        }
    return {}


def save_image_digests(digests: dict[str, str]) -> None:
    save_json_cache(IMAGE_DIGESTS_CACHE_PATH, digests)


def has_cached_data(tag: str) -> bool:
    return all(
        os.path.exists(os.path.join(CACHE_DIR, cache_type, f"{tag}.json"))
        for cache_type in ("functions", "keywords", "settings")
    )


def resolve_image_digests(tags: list[str]) -> dict[str, str]:
    digests = {}
    for tag in tags:
        if tag not in MUTABLE_TAGS:
            continue
        digest = get_remote_image_digest(tag)
        if digest:
            digests[tag] = digest
    return digests


def get_remote_image_digest(tag: str) -> str | None:
    token_response = requests.get(
        "https://auth.docker.io/token",
        params={
            "service": "registry.docker.io",
            "scope": "repository:clickhouse/clickhouse-server:pull",
        },
        timeout=30,
    )
    token_response.raise_for_status()
    token = token_response.json()["token"]

    manifest_response = requests.get(
        f"https://registry-1.docker.io/v2/clickhouse/clickhouse-server/manifests/{tag}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": ",".join(
                [
                    "application/vnd.oci.image.index.v1+json",
                    "application/vnd.docker.distribution.manifest.list.v2+json",
                    "application/vnd.oci.image.manifest.v1+json",
                    "application/vnd.docker.distribution.manifest.v2+json",
                ]
            ),
        },
        timeout=30,
    )
    manifest_response.raise_for_status()
    manifest = manifest_response.json()

    manifests = manifest.get("manifests")
    if isinstance(manifests, list):
        for entry in manifests:
            platform = entry.get("platform", {})
            if (
                platform.get("os") == "linux"
                and platform.get("architecture") == "amd64"
            ):
                return entry.get("digest")
        return manifest_response.headers.get("Docker-Content-Digest")

    return manifest_response.headers.get("Docker-Content-Digest")


def get_function_docs_urls(features: list[str]) -> dict[str, str]:
    """Return a function-name → docs-URL map, building and caching it on first call."""
    cached = load_json_cache(FUNCTION_DOCS_CACHE_PATH)
    requested_features = set(features)

    if isinstance(cached, dict) and requested_features.issubset(cached.keys()):
        logger.info("Loaded cached function docs URLs")
        return _apply_curated_overrides(cached, "function")

    if cached == {}:
        logger.info("Cached function docs URLs are empty, rebuilding cache")

    logger.info("Building function docs URL cache from documentation pages...")
    urls = dict(cached) if isinstance(cached, dict) else {}
    unresolved_features = []

    doc_pages = get_function_doc_pages()
    direct_doc_urls = get_direct_function_doc_urls(doc_pages)
    for feature in features:
        if feature in urls:
            continue
        normalized_feature = _normalize_feature_name(feature)
        direct_url = direct_doc_urls.get(normalized_feature)
        if direct_url is not None:
            urls[feature] = direct_url
        else:
            unresolved_features.append(feature)

    if unresolved_features:
        logger.info(
            f"Resolving {len(unresolved_features)} functions via docs page anchors..."
        )

        anchor_urls_by_feature = {}
        anchor_pages = get_anchor_function_doc_pages(doc_pages)
        with ThreadPoolExecutor(
            max_workers=min(DOCS_FETCH_WORKERS, len(anchor_pages) or 1)
        ) as executor:
            futures = {
                executor.submit(_fetch_page_ids, page_url): page_url
                for page_url in anchor_pages
            }
            for future in as_completed(futures):
                page_url, anchor_ids = future.result()
                for anchor in anchor_ids:
                    normalized_anchor = _normalize_feature_name(anchor)
                    anchor_urls_by_feature.setdefault(
                        normalized_anchor, f"{page_url}#{anchor}"
                    )

        for feature in unresolved_features:
            normalized_feature = _normalize_feature_name(feature)
            anchor_url = anchor_urls_by_feature.get(normalized_feature)
            if anchor_url is not None:
                urls[feature] = anchor_url
            else:
                logger.warning(f"No URL found for function {feature}")

    save_json_cache(FUNCTION_DOCS_CACHE_PATH, urls)
    return _apply_curated_overrides(urls, "function")


def get_setting_docs_urls(features: list[str]) -> dict[str, str]:
    cached = load_json_cache(SETTING_DOCS_CACHE_PATH)
    requested_features = set(features)

    if isinstance(cached, dict) and requested_features.issubset(cached.keys()):
        logger.info("Loaded cached setting docs URLs")
        return _apply_curated_overrides(cached, "setting")

    if cached == {}:
        logger.info("Cached setting docs URLs are empty, rebuilding cache")

    logger.info("Building setting docs URL cache from documentation page...")
    urls = dict(cached) if isinstance(cached, dict) else {}
    unresolved_features = [feature for feature in features if feature not in urls]

    if unresolved_features:
        _, anchor_ids = _fetch_page_ids(SETTINGS_DOCS_PAGE_URL)
        anchor_urls_by_feature = {
            _normalize_feature_name(anchor): f"{SETTINGS_DOCS_PAGE_URL}#{anchor}"
            for anchor in anchor_ids
        }

        for feature in unresolved_features:
            normalized_feature = _normalize_feature_name(feature)
            anchor_url = anchor_urls_by_feature.get(normalized_feature)
            if anchor_url is not None:
                urls[feature] = anchor_url
            else:
                logger.warning(f"No URL found for setting {feature}")

    save_json_cache(SETTING_DOCS_CACHE_PATH, urls)
    return _apply_curated_overrides(urls, "setting")


def get_keyword_docs_urls(features: list[str]) -> dict[str, str]:
    cached = load_json_cache(KEYWORD_DOCS_CACHE_PATH)
    requested_features = set(features)

    if isinstance(cached, dict) and requested_features.issubset(cached.keys()):
        logger.info("Loaded cached keyword docs URLs")
        return _apply_curated_overrides(cached, "keyword")

    if cached == {}:
        logger.info("Cached keyword docs URLs are empty, rebuilding cache")

    logger.info("Building keyword docs URL cache from statements documentation...")
    urls = dict(cached) if isinstance(cached, dict) else {}
    unresolved_features = [feature for feature in features if feature not in urls]

    if unresolved_features:
        statement_pages = get_statement_doc_pages()
        direct_doc_urls = get_direct_statement_doc_urls(statement_pages)
        anchor_urls_by_feature: dict[str, str] = {}

        with ThreadPoolExecutor(
            max_workers=min(DOCS_FETCH_WORKERS, len(statement_pages) or 1)
        ) as executor:
            futures = {
                executor.submit(_fetch_page_ids, page_url): page_url
                for page_url in statement_pages
            }
            for future in as_completed(futures):
                page_url, anchor_ids = future.result()
                for anchor in anchor_ids:
                    normalized_anchor = _normalize_feature_name(anchor)
                    anchor_urls_by_feature.setdefault(
                        normalized_anchor, f"{page_url}#{anchor}"
                    )

        for feature in unresolved_features:
            normalized_feature = _normalize_feature_name(feature)

            direct_url = direct_doc_urls.get(normalized_feature)
            if direct_url is not None:
                urls[feature] = direct_url
                continue

            anchor_url = anchor_urls_by_feature.get(normalized_feature)
            if anchor_url is not None:
                urls[feature] = anchor_url
                continue

            logger.warning(f"No URL found for keyword {feature}")

    save_json_cache(KEYWORD_DOCS_CACHE_PATH, urls)
    return _apply_curated_overrides(urls, "keyword")


def get_feature_docs_urls(feature_type: str, features: list[str]) -> dict[str, str]:
    if feature_type == "function":
        return get_function_docs_urls(features)
    if feature_type == "keyword":
        return get_keyword_docs_urls(features)
    if feature_type == "setting":
        return get_setting_docs_urls(features)
    return {}


def run_query(query: str, tag: str) -> str | None:
    if USE_FIDDLE:
        return run_query_against_fiddle(query, tag)
    return run_query_against_version_locally(query, tag)


def run_query_against_fiddle(query: str, tag: str) -> str | None:
    logger.info(f"Running query against Fiddle for {tag}")
    response = requests.post(
        "https://fiddle.clickhouse.com/api/runs",
        json={"query": query, "version": tag},
    )
    return response.json().get("result", {}).get("output")


def run_query_against_version_locally(query: str, tag: str) -> str | None:
    logger.info(f"Running query against local container for {tag}")
    container_name = CONTAINER_NAME_TEMPLATE.format(tag=tag)

    proc = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
        capture_output=True,
    )
    if not (proc.returncode == 0 and proc.stdout.decode().strip() == "true"):
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)
        logger.info(f"Pulling image for {tag}")
        if (
            subprocess.run(
                ["docker", "pull", f"clickhouse/clickhouse-server:{tag}"],
                capture_output=True,
            ).returncode
            != 0
        ):
            logger.error(f"Failed to pull image for {tag}")
        logger.info(f"Running container for {tag}")
        if (
            subprocess.run(
                [
                    "docker",
                    "run",
                    "--name",
                    container_name,
                    "-d",
                    f"clickhouse/clickhouse-server:{tag}",
                ],
                capture_output=True,
            ).returncode
            != 0
        ):
            logger.error(f"Failed to run container for {tag}")

    for n in range(60):
        logger.info(f"Running query for {tag} (attempt {n + 1})")
        proc = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                container_name,
                "clickhouse-client",
                "--query",
                query,
            ],
            capture_output=True,
        )
        stderr = proc.stderr.decode()
        if proc.returncode == 0 or "UNKNOWN_TABLE" in stderr:
            return proc.stdout.decode()

        if n > 10:
            logger.warning(f"Query failed, retrying ({n}): {stderr}")
        time.sleep(0.10)

    logger.error(f"Failed to run query for {tag}")
    return None


def get_tags(exclude_patch: bool = True, exclude_alpine: bool = True) -> list[str]:
    r = requests.get("https://fiddle.clickhouse.com/api/tags")
    tags = r.json().get("result", {}).get("tags", [])
    if exclude_patch:
        tags = [t for t in tags if t.count(".") == 1 or t in ALLOWED_TAGS]
    if exclude_alpine:
        tags = [t for t in tags if "alpine" not in t and "distroless" not in t]
    return tags


def render(
    version_info: dict,
    title: str = "ClickHouse Function Reference",
    header: str = "ClickHouse Function Availability Reference",
    feature_type: str = "function",
    filename: str = "index.html",
) -> None:
    alias_key = "alias_to" if feature_type == "function" else "alias_for"
    aliases = {
        feature["name"]: feature[alias_key]
        for features in version_info.values()
        for feature in features
        if alias_key in feature and feature[alias_key] != ""
    }

    all_features = sorted(
        {
            feature["name"]
            for features in version_info.values()
            for feature in features
            if "name" in feature
        }
    )

    versions = list(version_info.keys())

    feature_versions = {
        feature: {
            tag
            for tag, features in version_info.items()
            if any(f.get("name") == feature for f in features)
        }
        for feature in all_features
    }

    docs_links = get_feature_docs_urls(feature_type, all_features)

    features = []
    for feature in all_features:
        features.append(
            {
                "name": feature,
                "url": docs_links.get(feature)
                or docs_links.get(aliases.get(feature, "")),
                "alias_to": aliases.get(feature),
                "availability": [
                    version in feature_versions[feature] for version in versions
                ],
            }
        )

    generated_at = datetime.today().strftime("%Y-%m-%d %H:%M")
    data_path = DATA_DIR / f"{feature_type}s.json"
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(data_path, "w") as f:
        json.dump(
            {
                "title": title,
                "header": header,
                "feature_type": feature_type,
                "generated_at": generated_at,
                "versions": versions,
                "features": features,
            },
            f,
            separators=(",", ":"),
        )

    template = template_env.get_template("reference.html.j2")
    doc = template.render(
        title=title,
        header=header,
        feature_type=feature_type,
        data_url=f"assets/data/{feature_type}s.json",
        generated_at=generated_at,
    )

    with open(BASE_DIR / filename, "w") as f:
        f.write(doc)


if __name__ == "__main__":
    main()
