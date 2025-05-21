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

function generateColumns(viewName, nodeId) {
    const keys = [];

    if (nodeId === "total")
        keys.push("viewNodeId");

    const hasSchema = Object.values(REPORT_DATA['systemView']).some(nodeData => {
        if (nodeData[viewName]) {
            keys.push(...nodeData[viewName]['schema']);
            return true;
        }
        return false;
    });

    if (!hasSchema)
        return null;

    return keys.map((key, index) => ({
        field: index,
        title: key,
        sortable: true
    }));
}

function generateRows(viewName, nodeId) {
    if (nodeId !== "total") {
        const view = REPORT_DATA['systemView'][nodeId][viewName];
        if (view)
            return view['data'];

        return [];
    }

    return Object.entries(REPORT_DATA['systemView']).flatMap(([nodeId, nodeData]) => {
        if (!nodeData[viewName])
            return [];

        return nodeData[viewName]['data'].map(row => [nodeId, ...row]);
    });
}

function drawSystemViewsTable() {
    const div = document.getElementById('systemViewTableDiv');
    div.innerHTML = "";

    const nodeId = sysViewSearchNodesSelect.val();
    const viewName = searchViewsSelect.val();

    const columns = generateColumns(viewName, nodeId);
    const rows = generateRows(viewName, nodeId);

    if (rows.length === 0) {
        const heading = document.createElement('h4');
        heading.className = 'mt-4';
        heading.textContent = `No ${viewName} records found on node ${nodeId}.`;

        div.appendChild(heading);
        return;
    }

    const table = document.createElement('table');

    table.id = 'systemViewTable';
    div.appendChild(table);

    $('#systemViewTable').bootstrapTable({
        pagination: true,
        search: true,
        columns: columns,
        data: rows,
        sortOrder: 'desc'
    });
}

buildSelectNodesSystemView(sysViewSearchNodesSelect, drawSystemViewsTable);
buildSelectSystemViews(searchViewsSelect, drawSystemViewsTable);

drawSystemViewsTable();
