import csv
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from jinja2 import Environment, FileSystemLoader, select_autoescape
from loguru import logger

from scrape_docs import function_doc_template, function_pages

ALLOWED_TAGS = {"latest", "head"}
CACHE_DENY_LIST = {"latest", "head"}
USE_FIDDLE = False
WORKERS = 8  # concurrent Docker containers; ignored when USE_FIDDLE=True
CONTAINER_NAME_TEMPLATE = "clickhouse-function-reference-{tag}"
CACHE_DIR = "cache"
BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR / "templates"
template_env = Environment(
    loader=FileSystemLoader(TEMPLATES_DIR),
    autoescape=select_autoescape(["html", "xml"]),
)


def _process_version(version: str) -> tuple[str, list, list, list]:
    return (
        version,
        get_functions(version),
        get_keywords(version),
        get_settings(version),
    )


def main() -> None:
    tags = get_tags()
    versions = []
    for v in tags:
        versions.append(v)
        if v == "21.9":
            break

    workers = 1 if USE_FIDDLE else WORKERS
    results: dict[str, tuple] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_version, v): v for v in versions}
        for future in as_completed(futures):
            version, funcs, keywords, settings = future.result()
            results[version] = (funcs, keywords, settings)
            logger.info(f"Finished {version}")
            if not USE_FIDDLE:
                _cleanup_container(version)

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
        json.dump(data, f, indent=2)


def get_functions(tag: str) -> list[dict]:
    cache_path = os.path.join(CACHE_DIR, "functions", f"{tag}.json")
    if tag not in CACHE_DENY_LIST:
        cached = load_json_cache(cache_path)
        if cached is not None:
            logger.info(f"Loaded cached functions for {tag}")
            return cached

    tsv = run_query("SELECT * FROM system.functions FORMAT TabSeparatedWithNames", tag)
    if tsv is None:
        return []
    result = list(csv.DictReader(tsv.splitlines(), delimiter="\t"))
    if tag not in CACHE_DENY_LIST:
        save_json_cache(cache_path, result)
    return result


def get_keywords(tag: str) -> list[dict]:
    cache_path = os.path.join(CACHE_DIR, "keywords", f"{tag}.json")
    if tag not in CACHE_DENY_LIST:
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
    if tag not in CACHE_DENY_LIST:
        save_json_cache(cache_path, result)
    return result


def get_settings(tag: str) -> list[dict]:
    cache_path = os.path.join(CACHE_DIR, "settings", f"{tag}.json")
    if tag not in CACHE_DENY_LIST:
        cached = load_json_cache(cache_path)
        if cached is not None:
            logger.info(f"Loaded cached settings for {tag}")
            return cached

    tsv = run_query(
        "SELECT name FROM system.settings FORMAT TabSeparatedWithNames", tag
    )
    if tsv is None:
        return []
    result = list(csv.DictReader(tsv.splitlines(), delimiter="\t"))
    if tag not in CACHE_DENY_LIST:
        save_json_cache(cache_path, result)
    return result


def get_docs_urls(features: list[str]) -> dict[str, str]:
    """Return a function-name → docs-URL map, building and caching it on first call."""
    cache_path = os.path.join(CACHE_DIR, "docs_urls.json")
    cached = load_json_cache(cache_path)
    if cached is not None:
        logger.info("Loaded cached docs URLs")
        return cached

    logger.info("Building docs URL cache from documentation pages...")
    page_ref = {}
    for page in function_pages:
        logger.info(f"Fetching docs page: {page}")
        response = requests.get(function_doc_template.format(page=page))
        response.raise_for_status()
        page_ref[page] = response.text

    urls = {}
    for feature in features:
        std = feature.lower()
        for page, content in page_ref.items():
            if f'id="{std}"' in content:
                urls[feature] = f"{function_doc_template.format(page=page)}#{std}"
                break
            if f'id="{std.replace("_", "")}"' in content:
                urls[feature] = (
                    f"{function_doc_template.format(page=page)}#{std.replace('_', '')}"
                )
                break
        else:
            logger.warning(f"No URL found for function {feature}")

    save_json_cache(cache_path, urls)
    return urls


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
    aliases = {
        func["name"]: func["alias_to"]
        for funcs in version_info.values()
        for func in funcs
        if "alias_to" in func and func["alias_to"] != ""
    }

    all_features = sorted(
        {
            func["name"]
            for funcs in version_info.values()
            for func in funcs
            if "name" in func
        }
    )

    versions = list(version_info.keys())

    feature_versions = {
        feature: {
            tag
            for tag, funcs in version_info.items()
            if any(f.get("name") == feature for f in funcs)
        }
        for feature in all_features
    }

    docs_links = {}
    if feature_type == "function":
        docs_links = get_docs_urls(all_features)

    features = []
    for feature in all_features:
        cells = []
        for version in versions:
            is_available = version in feature_versions[feature]
            cells.append(
                {
                    "class_name": "avail" if is_available else "unavail",
                    "title": f"{feature} {'available' if is_available else 'not available'} in {version}",
                    "symbol": "✓" if is_available else "✗",
                }
            )

        features.append(
            {
                "name": feature,
                "url": docs_links.get(feature)
                or docs_links.get(aliases.get(feature, "")),
                "alias_to": aliases.get(feature),
                "cells": cells,
            }
        )

    template = template_env.get_template("reference.html.j2")
    doc = template.render(
        title=title,
        header=header,
        feature_type=feature_type,
        versions=versions,
        versions_json=json.dumps(versions),
        features=features,
        generated_at=datetime.today().strftime("%Y-%m-%d %H:%M"),
    )

    with open(BASE_DIR / filename, "w") as f:
        f.write(doc)


if __name__ == "__main__":
    main()
