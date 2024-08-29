import requests
import csv
import joblib
import jinja2

memory = joblib.Memory("cache", verbose=0)

def main() -> None:
    info = {}

    tags = get_tags()
    
    for tag in tags:
        print("Processing", tag)
        funcs = get_functions(tag)
        info[tag] = funcs
    
    render(info)

def run_query(query: str, tag: str) -> dict:
    json_data = {
    'query': query,
    'version': tag
}

    response = requests.post('https://fiddle.clickhouse.com/api/runs', json=json_data)
    return response.json().get("result", {}).get("output")

@memory.cache
def get_functions(tag: str) -> list[str]:
    tsv = run_query("SELECT * FROM system.functions FORMAT TabSeparatedWithNames", tag)
    reader = csv.DictReader(tsv.splitlines(), delimiter='\t')
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

def render(version_info: dict, filename:str = "funcs.html") -> None:

    # version_info is dict from tag to list of functions. pull out ALL functions and sort them so we can render them nicely

    # {"tag1": [{"name": "func1", "type": "Aggregate"}, {"name": "func2", "type": "Scalar"}], "tag2": [{"name": "func3", "type": "Aggregate"}]}
    # and we want e.g. {"func1", "func2", "func3"}

    all_funcs = sorted(list({func["name"] for funcs in version_info.values() for func in funcs}))

    # now we want a mapping from function name to tags where that version is present 

    # {"func1": ["tag1", "tag2"], "func2": ["tag1", "tag2"], "func3": ["tag2", "tag3"]}

    func_to_tags = {func: [tag for tag, funcs in version_info.items() if any(f["name"] == func for f in funcs)] for func in all_funcs}

    # now, drop functions that are present in /all/ versions from all_funcs (i.e. they are not interesting) 

    all_funcs = [func for func in all_funcs if len(func_to_tags[func]) != len(version_info)]
    
    doc = """<!doctype html>

<head>
    <meta charset="UTF-8">
    <title>ClickHouse Function Reference</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-QWTKZyjpPEjISv5WaRU9OFeRpok6YctnYmDr5pNlyT2bRjXh0JMhjY6hW+ALEwIH" crossorigin="anonymous">
</head>
<body>
<div class="container" style="max-width: none">
<h1> ClickHouse Function Availability Reference</h1>
"""

    # Each row is a version. Each column is a function. Each cell will indicate if the function is present in that version, with a coloured background

    doc += "<table><tr><th>Function</th>"
    for tag in version_info.keys():
        doc += f"<th>{tag}</th>"
    doc += "</tr>"
    for func in all_funcs:
        doc += f"<tr><td>{func}</td>"
        for tag in version_info.keys():
            if func in func_to_tags:
                if tag in func_to_tags[func]:
                    doc += "<td style='background-color: green'>✓</td>"
                else:
                    doc += "<td style='background-color: red'>✗</td>"
            else:
                doc += "<td>✗</td>"
        doc += "</tr>"
    doc += "</div>"
    doc += '<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js" integrity="sha384-YvpcrYf0tY3lHB60NNkmXc5s9fDVZLESaAA55NDzOxhy9GkcIdslK1eN7N6jIeHz" crossorigin="anonymous"></script>'
    doc += "</body>"

    with open(filename, "w") as f:
        f.write(doc)


if __name__ == "__main__":
    main()