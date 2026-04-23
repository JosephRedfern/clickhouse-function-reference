import re

import requests

DOCS_SITEMAP_URL = "https://clickhouse.com/docs/sitemap.xml"
DOCS_BASE_URL = "https://clickhouse.com/docs"
FUNCTIONS_PREFIX = "sql-reference/functions/"
AGGREGATE_FUNCTIONS_PREFIX = "sql-reference/aggregate-functions/"
AGGREGATE_REFERENCE_PREFIX = "sql-reference/aggregate-functions/reference/"


def _extract_urls_from_sitemap(xml_text: str) -> list[str]:
    return re.findall(r"<loc>(https://clickhouse\.com/docs/[^<]+)</loc>", xml_text)


def _normalize_feature_name(value: str) -> str:
    return value.lower().replace("_", "")


def _is_candidate_function_page(url: str) -> bool:
    if not url.startswith(f"{DOCS_BASE_URL}/sql-reference/"):
        return False

    path = url.removeprefix(f"{DOCS_BASE_URL}/")

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


def _get_path_from_url(url: str) -> str:
    return url.removeprefix(f"{DOCS_BASE_URL}/")


def _get_direct_doc_slug(url: str) -> str | None:
    path = _get_path_from_url(url)

    if path.startswith(AGGREGATE_REFERENCE_PREFIX):
        return path.removeprefix(AGGREGATE_REFERENCE_PREFIX).split("/", 1)[0]

    if path == "sql-reference/aggregate-functions/grouping_function":
        return "grouping"

    return None


def get_function_doc_pages() -> list[str]:
    response = requests.get(DOCS_SITEMAP_URL, timeout=30)
    response.raise_for_status()

    urls = _extract_urls_from_sitemap(response.text)
    pages = sorted(url for url in urls if _is_candidate_function_page(url))
    return pages


def get_direct_function_doc_urls(pages: list[str] | None = None) -> dict[str, str]:
    direct_urls = {}

    if pages is None:
        pages = get_function_doc_pages()

    for url in pages:
        slug = _get_direct_doc_slug(url)
        if slug is not None:
            direct_urls[_normalize_feature_name(slug)] = url

    return direct_urls


def get_anchor_function_doc_pages(pages: list[str] | None = None) -> list[str]:
    anchor_pages = []

    if pages is None:
        pages = get_function_doc_pages()

    for url in pages:
        if _get_direct_doc_slug(url) is None:
            anchor_pages.append(url)

    return anchor_pages
