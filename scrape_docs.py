import re

import requests

DOCS_SITEMAP_URL = "https://clickhouse.com/docs/sitemap.xml"
DOCS_BASE_URL = "https://clickhouse.com/docs"
FUNCTIONS_PREFIX = "sql-reference/functions/"
AGGREGATE_FUNCTIONS_PREFIX = "sql-reference/aggregate-functions/"
AGGREGATE_REFERENCE_PREFIX = "sql-reference/aggregate-functions/reference/"
STATEMENTS_PREFIX = "sql-reference/statements/"


def _extract_urls_from_sitemap(xml_text: str) -> list[str]:
    return re.findall(r"<loc>(https://clickhouse\.com/docs/[^<]+)</loc>", xml_text)


def _normalize_feature_name(value: str) -> str:
    return re.sub(r"[\s_\-/]+", "", value).lower()


def _get_path_from_url(url: str) -> str:
    return url.removeprefix(f"{DOCS_BASE_URL}/")


def get_docs_urls() -> list[str]:
    response = requests.get(DOCS_SITEMAP_URL, timeout=30)
    response.raise_for_status()
    return _extract_urls_from_sitemap(response.text)


def _is_candidate_function_page(url: str) -> bool:
    if not url.startswith(f"{DOCS_BASE_URL}/sql-reference/"):
        return False

    path = _get_path_from_url(url)

    allowed_prefixes = (
        FUNCTIONS_PREFIX,
        AGGREGATE_FUNCTIONS_PREFIX,
    )
    if not path.startswith(allowed_prefixes):
        return False

    excluded_paths = {
        "sql-reference/functions",
        "sql-reference/functions/overview",
        "sql-reference/functions/regular-functions",
        "sql-reference/aggregate-functions",
        "sql-reference/aggregate-functions/reference",
        "sql-reference/aggregate-functions/parametric-functions",
    }
    if path in excluded_paths:
        return False

    return True


def _is_candidate_statement_page(url: str) -> bool:
    if not url.startswith(f"{DOCS_BASE_URL}/{STATEMENTS_PREFIX}"):
        return False

    path = _get_path_from_url(url)

    excluded_paths = {
        "sql-reference/statements",
        "sql-reference/statements/",
    }
    if path in excluded_paths:
        return False

    return True


def _get_direct_function_doc_slug(url: str) -> str | None:
    path = _get_path_from_url(url)

    if path.startswith(AGGREGATE_REFERENCE_PREFIX):
        return path.removeprefix(AGGREGATE_REFERENCE_PREFIX).split("/", 1)[0]

    if path == "sql-reference/aggregate-functions/grouping_function":
        return "grouping"

    return None


def _get_statement_doc_slugs(url: str) -> list[str]:
    path = _get_path_from_url(url)
    if not path.startswith(STATEMENTS_PREFIX):
        return []

    relative_path = path.removeprefix(STATEMENTS_PREFIX).strip("/")
    if not relative_path:
        return []

    parts = [part for part in relative_path.split("/") if part]
    if not parts:
        return []

    slugs = []
    for i in range(1, len(parts) + 1):
        combined = " ".join(parts[:i]).replace("-", " ")
        slugs.append(combined)

    slugs.append(parts[-1].replace("-", " "))

    seen = set()
    result = []
    for slug in slugs:
        normalized = _normalize_feature_name(slug)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(slug)

    return result


def get_function_doc_pages() -> list[str]:
    urls = get_docs_urls()
    pages = sorted(url for url in urls if _is_candidate_function_page(url))
    return pages


def get_statement_doc_pages() -> list[str]:
    urls = get_docs_urls()
    pages = sorted(url for url in urls if _is_candidate_statement_page(url))
    return pages


def get_direct_function_doc_urls(pages: list[str] | None = None) -> dict[str, str]:
    direct_urls = {}

    if pages is None:
        pages = get_function_doc_pages()

    for url in pages:
        slug = _get_direct_function_doc_slug(url)
        if slug is not None:
            direct_urls[_normalize_feature_name(slug)] = url

    return direct_urls


def get_direct_statement_doc_urls(pages: list[str] | None = None) -> dict[str, str]:
    direct_urls = {}

    if pages is None:
        pages = get_statement_doc_pages()

    for url in pages:
        for slug in _get_statement_doc_slugs(url):
            direct_urls.setdefault(_normalize_feature_name(slug), url)

    return direct_urls


def get_anchor_function_doc_pages(pages: list[str] | None = None) -> list[str]:
    anchor_pages = []

    if pages is None:
        pages = get_function_doc_pages()

    for url in pages:
        if _get_direct_function_doc_slug(url) is None:
            anchor_pages.append(url)

    return anchor_pages
