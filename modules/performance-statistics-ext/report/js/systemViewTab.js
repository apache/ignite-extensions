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

const svSearchNodesSelect = $('#svSearchNodes');
const searchViewsSelect = $('#searchViews');

function getUniqueKeys(dataTable) {
    const keys = new Set();
    dataTable.forEach(function (row) {
        Object.keys(row).forEach(function (key) {
            keys.add(key);
        });
    });
    return Array.from(keys);
}

function generateColumns(keys) {
    const columns = [];

    keys.forEach(function (key) {
        columns.push({
            field: key,
            title: key.charAt(0).toUpperCase() + key.slice(1),
            sortable: true
        });
    });

    return columns;
}

function mergeData(viewName) {
    const mergedData = [];
    $.each(REPORT_DATA['systemView'], function (nodeId, data) {
        if (!data[viewName]) return;

        $.each(data[viewName], function (rowNumber, row) {
            const rowWithId = new Map();

            rowWithId['System View Node Id'] = nodeId;
            for (let key in row) rowWithId[key] = row[key];

            mergedData.push(rowWithId);
        });
    });

    return mergedData;
}

function drawSystemViewsTable() {
    const div = document.getElementById('systemViewTableDiv');
    div.innerHTML = "";

    const nodeId = svSearchNodesSelect.val();
    const viewName = searchViewsSelect.val();

    let data;

    if (nodeId === "total") data = mergeData(viewName);
    else data = REPORT_DATA['systemView'][nodeId][viewName];

    if (!data) {
        const heading = document.createElement('h2');
        heading.className = 'mt-4';
        heading.textContent = 'No data to display';

        div.appendChild(heading);
        return;
    }


    const uniqueKeys = getUniqueKeys(data);

    const columns = generateColumns(uniqueKeys);

    const table = document.createElement('table');

    table.id = 'systemViewTable';
    div.appendChild(table);

    $('#systemViewTable').bootstrapTable({
        pagination: true,
        search: true,
        columns: columns,
        data: data,
        sortName: uniqueKeys[0],
        sortOrder: 'desc'
    });
}

function update() {
    buildSelectSystemViews(searchViewsSelect, drawSystemViewsTable);
    drawSystemViewsTable();
}

buildSelectNodesSystemView(svSearchNodesSelect, update);
buildSelectSystemViews(searchViewsSelect, drawSystemViewsTable);

drawSystemViewsTable();
