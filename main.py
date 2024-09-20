import requests
import csv
import joblib
import json

from scrape_docs import function_pages, function_doc_template

memory = joblib.Memory("cache", verbose=0)

ALLOWED_TAGS = {"latest", "head"}


def main() -> None:
    function_info = {}
    keyword_info = {}

    tags = get_tags()

    for version in tags[:36]:
        print("Processing", version)
        funcs = get_functions(version)
        keywords = get_keywords(version)
        function_info[version] = funcs
        keyword_info[version] = keywords

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


def run_query(query: str, tag: str) -> dict:
    json_data = {"query": query, "version": tag}

    response = requests.post("https://fiddle.clickhouse.com/api/runs", json=json_data)
    return response.json().get("result", {}).get("output")


@memory.cache
def get_function_pages() -> dict[str, str]:
    page_ref = {}

    for page in function_pages:
        print("Processing", page)
        response = requests.get(function_doc_template.format(page=page))
        response.raise_for_status()

        page_ref[page] = response.text

    return page_ref


def get_url_for_function(function: str) -> str | None:
    # standardise function name
    std_func = function.lower()

    # pages contain anchor links to the functions, like #arrayjoin or #formatreadablesize.
    # we can iterate over pages, check for "#{std_func}" and return the page if found.
    # not every function is documented (https://github.com/ClickHouse/clickhouse-docs/issues/1833),
    # so we return None if we can't find the function.

    page_ref = get_function_pages()

    for page, content in page_ref.items():
        if f'id="{std_func}"' in content:
            return f"{function_doc_template.format(page=page)}#{std_func}"


@memory.cache
def get_functions(tag: str) -> list[str]:
    tsv = run_query("SELECT * FROM system.functions FORMAT TabSeparatedWithNames", tag)
    reader = csv.DictReader(tsv.splitlines(), delimiter="\t")
    return list(reader)


@memory.cache
def get_keywords(tag: str) -> list[str]:
    tsv = run_query(
        "SELECT keyword as name FROM system.keywords FORMAT TabSeparatedWithNames", tag
    )
    reader = csv.DictReader(tsv.splitlines(), delimiter="\t")
    return list(reader)


def get_tags(exclude_patch: bool = True, exclude_alpine: bool = True) -> list[str]:
    r = requests.get("https://fiddle.clickhouse.com/api/tags")
    tags = r.json().get("result", {}).get("tags", [])

    # if exclude_patch then strip the patch version, convert to set to remove duplicates

    if exclude_patch:
        tags = [t for t in tags if t.count(".") == 1 or t in ALLOWED_TAGS]

    if exclude_alpine:
        tags = [t for t in tags if "alpine" not in t]

    return tags


def render(
    version_info: dict,
    title: str = "ClickHouse Function Reference",
    header: str = "ClickHouse Function Availability Reference",
    feature_type: str = "function",
    filename: str = "index.html",
) -> None:

    # Mapping from altenative function names to the canonical name
    aliases = {
        func["name"]: func["alias_to"]
        for funcs in version_info.values()
        for func in funcs
        if "alias_to" in func and func["alias_to"] != ""
    }

    # Deduplicated list of all features
    all_features = sorted(
        list(
            {
                func["name"]
                for funcs in version_info.values()
                for func in funcs
                if "name" in func
            }
        )
    )

    # Mapping from feature to list of versions it is available in
    feature_to_versions = {
        feature: [
            tag
            for tag, funcs in version_info.items()
            if any(f.get("name") == feature for f in funcs)
        ]
        for feature in all_features
    }

    docs_links = (
        {
            feature: url
            for feature in all_features
            if (url := get_url_for_function(feature)) is not None
        }
        if feature_type == "function"
        else {}
    )

    doc = f"""<!doctype html>

<head>
    <meta charset="UTF-8">
    <title>{title}</title>    
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
    <!-- Google tag (gtag.js) -->
    <script async src="https://www.googletagmanager.com/gtag/js?id=G-W7W1TNNL21"></script>
    <script>
        window.dataLayer = window.dataLayer || [];
        function gtag(){{dataLayer.push(arguments);}}
        gtag('js', new Date());

        gtag('config', 'G-W7W1TNNL21');
    </script>
    <script>
    var availability = {json.dumps(feature_to_versions, indent=4)};
    var docs = {json.dumps(docs_links, indent=4)};
    var aliases = {json.dumps(aliases, indent=4)};
    </script>
    <style>
    body {{
        font-size: 13px;
    }}

    .main{{
        margin-top: 20px;
    }}
    </style>

</head>
<body>
<div class="container" style="max-width: none">
<h1>{header}</h1>
<nav>[ <a href="index.html">Function Reference</a> | <a href="keywords.html">Keyword Reference</a> ]</nav>
<div class="main">
"""

    doc += f"""
    <input type="text" id="search" onkeyup="search()" placeholder="Search for {feature_type}s...">
    """

    doc += """
    <script>
    function search() {
        const input = document.getElementById('search').value.toUpperCase();
        const table = document.getElementById('feature_table');
        const rows = table.getElementsByTagName('tr');
        for (let i = 1; i < rows.length; i++) {
            const cells = rows[i].getElementsByTagName('td');
            let found = false;
            for (const cell of cells) {
                if (cell.textContent.toUpperCase().includes(input)) {
                    found = true;
                    break;
                }
            }
            if (found) {
                rows[i].style.display = '';
            } else {
                rows[i].style.display = 'none';
            }
        }
    }
    </script>
    """

    doc += "<table id='feature_table'></table>"

    # Using Javascript,  populate the table with the data we have in availability

    doc += "<script>"
    doc += """
    const table = document.getElementById('feature_table');
    const features = Object.keys(availability);
    const versions = [...new Set(Object.values(availability).flat())];
    const header = table.createTHead();
    const headerRow = header.insertRow(0);
    headerRow.insertCell(0);

    for (const version of versions) {
        const cell = headerRow.insertCell();
        cell.textContent = version;
    }

    for (const feature of features) {
        const row = table.insertRow();
        const cell = row.insertCell();

        var url = null; 
        
        // direct link to docs
        if (docs.hasOwnProperty(feature)) {
            url = document.createElement('a');
        } else {
            if (aliases.hasOwnProperty(feature)) {
               // no direct link to docs, check if there is an alias
                url = docs[aliases[feature]];
            }
        }

        if (url) {
            cell.innerHTML = `<a href="${url}">${feature}</a>`;
        } else {
            cell.innerHTML = feature;
        }

        if (aliases.hasOwnProperty(feature)) {
            cell.innerHTML += "*";
        }

        for (const version of versions) {
            const cell = row.insertCell();
            if (availability[feature].includes(version)) {
                cell.textContent = '✓';
                cell.style.backgroundColor = 'green';
            } else {
                cell.textContent = '✗';
                cell.style.backgroundColor = 'red';
            }
            cell.onmouseover = () => showTooltip(cell, version, feature);
            cell.onmouseout = () => hideTooltip(cell);
        }
    }

    function showTooltip(cell, version, feature) {
        const tooltip = document.createElement('div');
        tooltip.style.position = 'absolute';
        tooltip.style.backgroundColor = 'black';
        tooltip.style.color = 'white';
        tooltip.style.padding = '10px';
        tooltip.style.borderRadius = '5px';
        tooltip.style.zIndex = '1000';
        
        if (availability[feature].includes(version)) {
            tooltip.textContent = `${feature} is available in ${version}`;
        } else {
            tooltip.textContent = `${feature} is not available in ${version}`;
        }
        
        cell.appendChild(tooltip);
    }

    function hideTooltip(cell) {
        cell.removeChild(cell.lastChild);
    }

    """
    doc += "</script>"
    doc += '<p style="margin-top: 15px">* indicates an alias to another function</p>'

    doc += "</div>"
    doc += "</div>"

    doc += "<div style='margin-top: 5px'><footer><p>Source on <a href='https://github.com/JosephRedfern/clickhouse-function-reference'>GitHub</a></p></footer></div>"
    doc += '<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js" integrity="sha384-YvpcrYf0tY3lHB60NNkmXc5s9fDVZLESaAA55NDzOxhy9GkcIdslK1eN7N6jIeHz" crossorigin="anonymous"></script>'
    doc += "</body>"

    with open(filename, "w") as f:
        f.write(doc)


if __name__ == "__main__":
    main()
