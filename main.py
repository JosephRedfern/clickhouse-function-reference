import subprocess
import requests
from datetime import datetime
import time
import csv
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import escape
from loguru import logger

from scrape_docs import function_pages, function_doc_template

ALLOWED_TAGS = {"latest", "head"}
CACHE_DENY_LIST = {"latest", "head"}
USE_FIDDLE = False
WORKERS = 8  # concurrent Docker containers; ignored when USE_FIDDLE=True
CONTAINER_NAME_TEMPLATE = "clickhouse-function-reference-{tag}"
CACHE_DIR = "cache"


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
        ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER_NAME_TEMPLATE.format(tag=version)],
        capture_output=True,
    )
    if proc.returncode == 0 and proc.stdout.decode().strip() == "true":
        logger.info(f"Stopping container for {version}")
        subprocess.run(["docker", "stop", CONTAINER_NAME_TEMPLATE.format(tag=version)], capture_output=True)
        subprocess.run(["docker", "rm", CONTAINER_NAME_TEMPLATE.format(tag=version)], capture_output=True)


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
                urls[feature] = f"{function_doc_template.format(page=page)}#{std.replace('_', '')}"
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
        if subprocess.run(
            ["docker", "pull", f"clickhouse/clickhouse-server:{tag}"],
            capture_output=True,
        ).returncode != 0:
            logger.error(f"Failed to pull image for {tag}")
        logger.info(f"Running container for {tag}")
        if subprocess.run(
            ["docker", "run", "--name", container_name, "-d", f"clickhouse/clickhouse-server:{tag}"],
            capture_output=True,
        ).returncode != 0:
            logger.error(f"Failed to run container for {tag}")

    for n in range(60):
        logger.info(f"Running query for {tag} (attempt {n + 1})")
        proc = subprocess.run(
            ["docker", "exec", "-i", container_name, "clickhouse-client", "--query", query],
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

    all_features = sorted({
        func["name"]
        for funcs in version_info.values()
        for func in funcs
        if "name" in func
    })

    versions = list(version_info.keys())

    feature_versions = {
        feature: {tag for tag, funcs in version_info.items() if any(f.get("name") == feature for f in funcs)}
        for feature in all_features
    }

    docs_links = {}
    if feature_type == "function":
        docs_links = get_docs_urls(all_features)

    # Build version header cells (1-based colIndex matches toggleCol() in JS)
    header_cells = "\n            ".join(
        f'<th class="ver-col" onclick="toggleCol({i + 1})" title="Click to hide {escape(v)}"><span>{escape(v)}</span></th>'
        for i, v in enumerate(versions)
    )

    def build_row(feature: str) -> str:
        url = docs_links.get(feature) or docs_links.get(aliases.get(feature, ""))
        safe = escape(feature)
        name_html = f'<a href="{escape(url)}">{safe}</a>' if url else safe
        if feature in aliases:
            name_html += f'<span class="alias-mark" title="Alias for {escape(aliases[feature])}">*</span>'

        cells = [f'<td class="name-col">{name_html}</td>']
        for v in versions:
            if v in feature_versions[feature]:
                cells.append(f'<td class="avail" title="{safe} available in {v}">✓</td>')
            else:
                cells.append(f'<td class="unavail" title="{safe} not available in {v}">✗</td>')
        return f'<tr data-name="{escape(feature.lower())}">' + "".join(cells) + "</tr>"

    rows_html = "\n            ".join(build_row(f) for f in all_features)
    versions_json = json.dumps(versions)

    doc = f"""<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{escape(title)}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
    <script async src="https://www.googletagmanager.com/gtag/js?id=G-W7W1TNNL21"></script>
    <script>
        window.dataLayer = window.dataLayer || [];
        function gtag(){{dataLayer.push(arguments);}}
        gtag('js', new Date());
        gtag('config', 'G-W7W1TNNL21');
    </script>
    <style>
    html, body {{
        height: 100%;
        margin: 0;
    }}
    body {{
        font-size: 13px;
        display: flex;
        flex-direction: column;
    }}
    .page-wrap {{
        display: flex;
        flex-direction: column;
        flex: 1;
        min-height: 0;
        padding: 12px 16px;
    }}
    h1 {{
        font-size: 1.6rem;
        margin-bottom: 4px;
    }}
    .main {{
        display: flex;
        flex-direction: column;
        flex: 1;
        min-height: 0;
        margin-top: 10px;
    }}
    .table-wrapper {{
        flex: 1;
        min-height: 0;
        overflow: auto;
        border: 1px solid #dee2e6;
        border-radius: 4px;
    }}
    #feature_table {{
        border-collapse: separate;
        border-spacing: 0;
        margin: 0;
        width: max-content;
    }}
    /* Sticky header row — applied to thead as a unit, not individual th cells */
    #feature_table thead {{
        position: sticky;
        top: 0;
        z-index: 2;
    }}
    #feature_table thead th {{
        background: #f1f3f5;
        border-bottom: 2px solid #ced4da;
    }}
    /* Sticky name column */
    .name-col {{
        position: sticky;
        left: 0;
        z-index: 1;
        background: white;
        min-width: 180px;
        padding: 3px 8px;
        border-right: 1px solid #dee2e6;
        white-space: nowrap;
    }}
    thead .name-col {{
        z-index: 3;
        background: #f1f3f5;
    }}
    /* Rotated version column headers */
    th.ver-col {{
        vertical-align: bottom;
        padding: 8px 4px;
        cursor: pointer;
        user-select: none;
        width: 28px;
        min-width: 28px;
        max-width: 28px;
        text-align: center;
        font-weight: 500;
        overflow: hidden;
    }}
    th.ver-col:hover {{
        background: #e9ecef;
    }}
    th.ver-col span {{
        display: inline-block;
        writing-mode: vertical-rl;
        white-space: nowrap;
    }}
    /* Availability cells */
    td.avail, td.unavail {{
        text-align: center;
        width: 28px;
        min-width: 28px;
        max-width: 28px;
        padding: 2px 0;
    }}
    td.avail {{
        background: #d4edda;
        color: #155724;
    }}
    td.unavail {{
        background: #f8f9fa;
        color: #ced4da;
    }}
    #feature_table tbody tr:hover .name-col {{
        background: #f8f9fa;
    }}
    #feature_table tbody tr:hover td.avail {{
        background: #b8dfc4;
    }}
    #feature_table tbody tr:hover td.unavail {{
        background: #eef0f2;
    }}
    .alias-mark {{
        color: #6c757d;
        font-size: 0.8em;
        margin-left: 1px;
    }}
    #restore-btns {{
        margin-bottom: 6px;
    }}
    footer {{
        font-size: 0.8rem;
        color: #6c757d;
        padding-top: 8px;
        flex-shrink: 0;
    }}
    </style>
</head>
<body>
<div class="page-wrap">
    <h1>{escape(header)}</h1>
    <nav class="mb-1">[ <a href="index.html">Function Reference</a> | <a href="keywords.html">Keyword Reference</a> | <a href="settings.html">Setting Reference</a> ]</nav>
    <p class="text-muted mb-1" style="font-size:0.85rem;">Availability across recent ClickHouse releases, sourced from <code>system.functions</code>, <code>system.keywords</code>, and <code>system.settings</code>.</p>
    <div class="main">
        <input type="text" id="search" class="form-control form-control-sm mb-2" oninput="filterRows()" placeholder="Search for {escape(feature_type)}s...">
        <div id="restore-btns"></div>
        <div class="table-wrapper">
            <table id="feature_table" class="table table-sm mb-0">
                <thead>
                    <tr>
                        <th class="name-col"></th>
                        {header_cells}
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        <p class="mt-1 text-muted" style="font-size:0.75rem;">* indicates an alias to another {escape(feature_type)}</p>
    </div>
    <footer>
        Source on <a href="https://github.com/JosephRedfern/clickhouse-function-reference">GitHub</a> &middot; last updated {datetime.today().strftime('%Y-%m-%d %H:%M')}
    </footer>
</div>
<script>
    const versions = {versions_json};
    const hiddenCols = new Set();

    function filterRows() {{
        const term = document.getElementById('search').value.toLowerCase();
        for (const row of document.querySelectorAll('#feature_table tbody tr')) {{
            row.style.display = row.dataset.name.includes(term) ? '' : 'none';
        }}
    }}

    function toggleCol(colIndex) {{
        // colIndex is 1-based (1 = first version column, after the name column)
        const cells = document.querySelectorAll(`#feature_table tr > :nth-child(${{colIndex + 1}})`);
        if (hiddenCols.has(colIndex)) {{
            hiddenCols.delete(colIndex);
            cells.forEach(c => c.style.display = '');
        }} else {{
            hiddenCols.add(colIndex);
            cells.forEach(c => c.style.display = 'none');
        }}
        renderRestoreButtons();
    }}

    function renderRestoreButtons() {{
        const container = document.getElementById('restore-btns');
        container.innerHTML = '';
        for (const colIndex of [...hiddenCols].sort((a, b) => a - b)) {{
            const btn = document.createElement('button');
            btn.className = 'btn btn-outline-secondary btn-sm me-1 mb-1';
            btn.textContent = `+ ${{versions[colIndex - 1]}}`;
            btn.onclick = () => toggleCol(colIndex);
            container.appendChild(btn);
        }}
    }}
</script>
</body>
</html>
"""

    with open(filename, "w") as f:
        f.write(doc)


if __name__ == "__main__":
    main()
