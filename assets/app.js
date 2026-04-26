(() => {
    const body = document.body;
    const dataUrl = body.dataset.dataUrl;
    const featureType = body.dataset.featureType || "feature";

    const searchInput = document.getElementById("search");
    const loadingEl = document.getElementById("loading");
    const errorEl = document.getElementById("error-message");
    const restoreBtnsEl = document.getElementById("restore-btns");
    const tableEl = document.getElementById("feature_table");
    const theadEl = tableEl.querySelector("thead");
    const tbodyEl = tableEl.querySelector("tbody");
    const generatedAtEl = document.getElementById("generated-at");

    const hiddenCols = new Set();
    let versions = [];
    let rows = [];

    function showError(message) {
        if (loadingEl) {
            loadingEl.style.display = "none";
        }
        if (errorEl) {
            errorEl.textContent = message;
            errorEl.style.display = "block";
        }
    }

    function clearError() {
        if (errorEl) {
            errorEl.textContent = "";
            errorEl.style.display = "none";
        }
    }

    function setLoading(isLoading) {
        if (loadingEl) {
            loadingEl.style.display = isLoading ? "" : "none";
        }
        if (searchInput) {
            searchInput.disabled = isLoading;
        }
        if (tableEl) {
            tableEl.hidden = isLoading;
        }
    }

    function featureLabel(feature) {
        return feature.alias_to
            ? `${feature.name} (alias for ${feature.alias_to})`
            : feature.name;
    }

    function applyFilter() {
        const term = (searchInput?.value || "").trim().toLowerCase();
        for (const row of rows) {
            row.style.display = row.dataset.name.includes(term) ? "" : "none";
        }
    }

    function renderRestoreButtons() {
        restoreBtnsEl.innerHTML = "";

        for (const colIndex of [...hiddenCols].sort((a, b) => a - b)) {
            const btn = document.createElement("button");
            btn.type = "button";
            btn.className = "btn btn-outline-secondary btn-sm me-1 mb-1";
            btn.textContent = `+ ${versions[colIndex - 1]}`;
            btn.addEventListener("click", () => toggleCol(colIndex));
            restoreBtnsEl.appendChild(btn);
        }
    }

    function toggleCol(colIndex) {
        const cells = tableEl.querySelectorAll(
            `tr > :nth-child(${colIndex + 1})`
        );

        if (hiddenCols.has(colIndex)) {
            hiddenCols.delete(colIndex);
            cells.forEach((cell) => {
                cell.style.display = "";
            });
        } else {
            hiddenCols.add(colIndex);
            cells.forEach((cell) => {
                cell.style.display = "none";
            });
        }

        renderRestoreButtons();
    }

    function buildHeader() {
        const headerRow = document.createElement("tr");

        const nameHeader = document.createElement("th");
        nameHeader.className = "name-col";
        headerRow.appendChild(nameHeader);

        versions.forEach((version, index) => {
            const th = document.createElement("th");
            th.className = "ver-col";
            th.title = `Click to hide ${version}`;
            th.addEventListener("click", () => toggleCol(index + 1));

            const span = document.createElement("span");
            span.textContent = version;
            th.appendChild(span);

            headerRow.appendChild(th);
        });

        theadEl.replaceChildren(headerRow);
    }

    function buildBody(features) {
        const fragment = document.createDocumentFragment();

        for (const feature of features) {
            const tr = document.createElement("tr");
            tr.dataset.name = feature.name.toLowerCase();

            const nameCell = document.createElement("td");
            nameCell.className = "name-col";

            if (feature.url) {
                const link = document.createElement("a");
                link.href = feature.url;
                link.textContent = feature.name;
                nameCell.appendChild(link);
            } else {
                nameCell.appendChild(document.createTextNode(feature.name));
            }

            if (feature.alias_to) {
                const aliasMark = document.createElement("span");
                aliasMark.className = "alias-mark";
                aliasMark.title = `Alias for ${feature.alias_to}`;
                aliasMark.textContent = "*";
                nameCell.appendChild(aliasMark);
            }

            tr.appendChild(nameCell);

            const availability = Array.isArray(feature.availability)
                ? feature.availability
                : [];

            versions.forEach((version, versionIndex) => {
                const isAvailable = Boolean(availability[versionIndex]);
                const td = document.createElement("td");
                td.className = isAvailable ? "avail" : "unavail";
                td.title = `${featureLabel(feature)} ${isAvailable ? "available" : "not available"} in ${version}`;
                td.textContent = isAvailable ? "✓" : "✗";
                tr.appendChild(td);
            });

            fragment.appendChild(tr);
        }

        tbodyEl.replaceChildren(fragment);
        rows = Array.from(tbodyEl.querySelectorAll("tr"));
    }

    async function loadData() {
        if (!dataUrl) {
            showError("Missing data source for this page.");
            return;
        }

        setLoading(true);
        clearError();

        try {
            const response = await fetch(dataUrl);
            if (!response.ok) {
                throw new Error(`Request failed with status ${response.status}`);
            }

            const data = await response.json();

            versions = Array.isArray(data.versions) ? data.versions : [];
            const features = Array.isArray(data.features) ? data.features : [];

            buildHeader();
            buildBody(features);
            renderRestoreButtons();
            applyFilter();

            if (generatedAtEl && data.generated_at) {
                generatedAtEl.textContent = data.generated_at;
            }

            if (data.header) {
                document.title = data.title || document.title;
                const heading = document.querySelector("h1");
                if (heading) {
                    heading.textContent = data.header;
                }
            }

            setLoading(false);
        } catch (error) {
            console.error("Failed to load reference data:", error);
            showError(
                `Failed to load ${featureType} data. ${error instanceof Error ? error.message : "Unknown error."}`
            );
        }
    }

    if (searchInput) {
        searchInput.addEventListener("input", applyFilter);
    }

    loadData();
})();
