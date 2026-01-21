/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const sysViewSearchNodesSelect = $('#sysViewSearchNodes');
const searchViewsSelect = $('#searchViews');

function getSystemViewMeta(viewName) {
    return `data/system-views/${viewName}/meta.json`;
}

function getSystemViewChunk(viewName, nodeId, chunkIdx) {
    const chunkName = String(chunkIdx).padStart(6, '0');
    return `data/system-views/${viewName}/${nodeId}/chunk-${chunkName}.json`;
}

function loadSystemViewMeta(viewName) {
    return $.getJSON(getSystemViewMeta(viewName)).then(meta => {
        meta.nodeOrder = Object.keys(meta.nodes).sort();
        meta.totalCount = meta.nodeOrder.reduce((sum, nodeId) => sum + Number(meta.nodes[nodeId]), 0);
        meta.totalSegments = buildTotalSegments(meta);
        return meta;
    });
}

function loadSystemViewChunk(viewName, nodeId, chunkIdx) {
    return $.getJSON(getSystemViewChunk(viewName, nodeId, chunkIdx)).then(chunk => {
        const rows = (chunk && Array.isArray(chunk.rows)) ? chunk.rows : [];

        return rows;
    });
}

function buildTotalSegments(meta) {
    let offset = 0;

    return meta.nodeOrder.map(nodeId => {
        const count = Number(meta.nodes[nodeId]);
        const start = offset;
        const end = offset + count;

        offset = end;

        return { nodeId, start, end };
    }).filter(segment => segment.end > segment.start);
}

function fetchRowsForRange(viewName, nodeId, meta, start, end) {
    if (end <= start)
        return Promise.resolve([]);

    const chunkSize = meta.chunkSize;
    const startChunk = Math.floor(start / chunkSize);
    const endChunk = Math.floor((end - 1) / chunkSize);
    const chunkRequests = [];

    for (let idx = startChunk; idx <= endChunk; idx++)
        chunkRequests.push(loadSystemViewChunk(viewName, nodeId, idx));

    return Promise.all(chunkRequests).then(chunks => {
        const rows = [];

        chunks.forEach((chunkRows, index) => {
            const chunkIdx = startChunk + index;
            const chunkStart = chunkIdx * chunkSize;
            const sliceStart = Math.max(start, chunkStart) - chunkStart;
            const sliceEnd = Math.min(end, chunkStart + chunkRows.length) - chunkStart;

            if (sliceEnd > sliceStart)
                rows.push(...chunkRows.slice(sliceStart, sliceEnd));
        });

        return rows;
    });
}

function fetchTotalRows(viewName, meta, start, end) {
    if (end <= start)
        return Promise.resolve([]);

    const requests = [];

    meta.totalSegments.forEach(segment => {
        if (end <= segment.start || start >= segment.end)
            return;

        const localStart = Math.max(start, segment.start) - segment.start;
        const localEnd = Math.min(end, segment.end) - segment.start;

        requests.push({ nodeId: segment.nodeId, localStart, localEnd });
    });

    return Promise.all(requests.map(req => {
        return fetchRowsForRange(viewName, req.nodeId, meta, req.localStart, req.localEnd)
            .then(rows => rows.map(row => [req.nodeId, ...row]));
    })).then(chunks => chunks.flat());
}

function generateColumns(meta, nodeId) {
    if (!meta || !Array.isArray(meta.columns) || meta.columns.length === 0)
        return [];

    const columns = [];

    if (nodeId === "total")
        columns.push({ field: 0, title: "nodeId", sortable: false });

    meta.columns.forEach((key, index) => {
        const field = nodeId === "total" ? index + 1 : index;

        columns.push({
            field: field,
            title: key,
            sortable: false
        });
    });

    return columns;
}

function drawSystemViewsTable() {
    const div = document.getElementById('systemViewTableDiv');
    div.innerHTML = "";

    const nodeId = sysViewSearchNodesSelect.val();
    const viewName = searchViewsSelect.val();

    loadSystemViewMeta(viewName).then(viewMeta => {
        const columns = generateColumns(viewMeta, nodeId);
        const table = getOrCreateSystemViewTable(div);
        const totalRows = nodeId === "total" ? viewMeta.totalCount : viewMeta.nodes[nodeId];

        $(table).bootstrapTable({
            formatNoMatches: () => `The "${viewName}" system view is empty on node "${nodeId}".`,
            pagination: true,
            sidePagination: "server",
            search: false,
            columns: columns,
            sortOrder: 'desc',
            ajax: function (params) {
                const limit = params.data.limit;
                const offset = params.data.offset;
                const end = Math.min(offset + limit, totalRows);

                if (totalRows === 0) {
                    params.success({total: 0, rows: []});
                    return;
                }

                const loader = nodeId === "total"
                    ? fetchTotalRows(viewName, viewMeta, offset, end)
                    : fetchRowsForRange(viewName, nodeId, viewMeta, offset, end);

                loader.then(rows => {
                    console.log('[systemView] Loaded rows', {viewName, nodeId, count: rows.length});
                    params.success({total: totalRows, rows: rows});
                }).catch(err => {
                    console.error('[systemView] Failed to load rows', viewName, nodeId, err);
                    params.error(err);
                });
            }
        });
    });
}

function getOrCreateSystemViewTable(div) {
    let table = document.getElementById('systemViewTable');

    if (!table) {
        div.innerHTML = "";
        table = document.createElement('table');
        table.id = 'systemViewTable';
        div.appendChild(table);
    }

    return table;
}

buildSelectNodesSystemView(sysViewSearchNodesSelect, drawSystemViewsTable);
buildSelectSystemViews(searchViewsSelect, drawSystemViewsTable);

drawSystemViewsTable();
