import requests
import csv
import joblib
import json

memory = joblib.Memory("cache", verbose=0)


def main() -> None:
    info = {}

    tags = get_tags()

    for tag in tags[:36]:
        print("Processing", tag)
        funcs = get_functions(tag)
        info[tag] = funcs

    render(info)


def run_query(query: str, tag: str) -> dict:
    json_data = {"query": query, "version": tag}

    response = requests.post("https://fiddle.clickhouse.com/api/runs", json=json_data)
    return response.json().get("result", {}).get("output")


@memory.cache
def get_functions(tag: str) -> list[str]:
    tsv = run_query("SELECT * FROM system.functions FORMAT TabSeparatedWithNames", tag)
    reader = csv.DictReader(tsv.splitlines(), delimiter="\t")
    return list(reader)


def get_tags(exclude_patch: bool = True, exclude_alpine: bool = True) -> list[str]:
    r = requests.get("https://fiddle.clickhouse.com/api/tags")
    tags = r.json().get("result", {}).get("tags", [])

    # if exclude_patch then strip the patch version, convert to set to remove duplicates

    if exclude_patch:
        tags = [t for t in tags if t.count(".") == 1]

    if exclude_alpine:
        tags = [t for t in tags if "alpine" not in t]

    return tags


def render(version_info: dict, filename: str = "index.html") -> None:
    all_funcs = sorted(
        list({func["name"] for funcs in version_info.values() for func in funcs})
    )
    func_to_tags = {
        func: [
            tag
            for tag, funcs in version_info.items()
            if any(f["name"] == func for f in funcs)
        ]
        for func in all_funcs
    }

    doc = f"""<!doctype html>

<head>
    <meta charset="UTF-8">
    <title>ClickHouse Function Reference</title>    
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
    var function_availability = {json.dumps(func_to_tags, indent=4)};
    </script>
    <style>
    body {{
        font-size: 13px;
    }}
    </style>

</head>
<body>
<div class="container" style="max-width: none">
<h1> ClickHouse Function Availability Reference</h1>
"""

    doc += """
    <input type="text" id="search" onkeyup="search()" placeholder="Search for functions...">
    <script>
    function search() {
        const input = document.getElementById('search').value.toUpperCase();
        const table = document.getElementById('function_table');
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

    doc += "<table id='function_table'></table>"

    # Using Javascript,  populate the table with the data we have in function_availability

    doc += "<script>"
    doc += """
    const table = document.getElementById('function_table');
    const functions = Object.keys(function_availability);
    const tags = [...new Set(Object.values(function_availability).flat())];
    const header = table.createTHead();
    const headerRow = header.insertRow(0);
    headerRow.insertCell(0);
    for (const tag of tags) {
        const cell = headerRow.insertCell();
        cell.textContent = tag;
    }
    for (const func of functions) {
        const row = table.insertRow();
        const cell = row.insertCell();
        cell.textContent = func;
        for (const tag of tags) {
            const cell = row.insertCell();
            if (function_availability[func].includes(tag)) {
                cell.textContent = '✓';
                cell.style.backgroundColor = 'green';
            } else {
                cell.textContent = '✗';
                cell.style.backgroundColor = 'red';
            }
            cell.onmouseover = () => showTooltip(cell, tag, func);
            cell.onmouseout = () => hideTooltip(cell);
        }
    }

    function showTooltip(cell, tag, func) {
        const tooltip = document.createElement('div');
        tooltip.style.position = 'absolute';
        tooltip.style.backgroundColor = 'black';
        tooltip.style.color = 'white';
        tooltip.style.padding = '10px';
        tooltip.style.borderRadius = '5px';
        tooltip.style.zIndex = '1000';
        
        if (function_availability[func].includes(tag)) {
            tooltip.textContent = `${func} is available in ${tag}`;
        } else {
            tooltip.textContent = `${func} is not available in ${tag}`;
        }
        
        cell.appendChild(tooltip);
    }

    function hideTooltip(cell) {
        cell.removeChild(cell.lastChild);
    }

    """
    doc += "</script>"

    doc += "</div>"

    with open(filename, "w") as f:
        f.write(doc)

    doc += "<div style='margin-top: 5px'><footer><p>Source on <a href='https://github.com/JosephRedfern/clickhouse-function-reference'>GitHub</a></p></footer></div>"
    doc += '<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js" integrity="sha384-YvpcrYf0tY3lHB60NNkmXc5s9fDVZLESaAA55NDzOxhy9GkcIdslK1eN7N6jIeHz" crossorigin="anonymous"></script>'
    doc += "</body>"

    with open(filename, "w") as f:
        f.write(doc)


if __name__ == "__main__":
    main()
