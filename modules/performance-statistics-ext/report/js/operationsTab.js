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

const CACHE_OPERATIONS = ["CACHE_GET", "CACHE_PUT", "CACHE_REMOVE", "CACHE_GET_AND_PUT", "CACHE_GET_AND_REMOVE",
    "CACHE_GET_ALL", "CACHE_PUT_ALL", "CACHE_REMOVE_ALL", "CACHE_INVOKE", "CACHE_INVOKE_ALL", "CACHE_PUT_ALL_CONFLICT",
    "CACHE_REMOVE_ALL_CONFLICT", "CACHE_LOCK"];

const CACHE_OPERATIONS_READABLE = ["get", "put", "remove", "getAndPut", "getAndRemove",
    "getAll", "putAll", "removeAll", "invoke", "invokeAll", "putAllConflict", "removeAllConflict", "lock"];

const CACHE_STORE_OPERATIONS = ["CACHE_LOAD", "CACHE_LOAD_ALL", "CACHE_LOAD_CACHE",
    "CACHE_WRITE", "CACHE_WRITE_ALL", "CACHE_DELETE", "CACHE_DELETE_ALL"];

const CACHE_STORE_OPERATIONS_READABLE = ["load", "loadAll", "loadCache", "write", "writeAll", "delete", "deleteAll"];

const CACHE_OPERATIONS_COLORS = {
    CACHE_GET: "#007bff",
    CACHE_PUT: "#4661EE",
    CACHE_REMOVE: "#EC5657",
    CACHE_GET_AND_PUT: "#c02332",
    CACHE_GET_AND_REMOVE: "#9d71e4",
    CACHE_GET_ALL: "#8357c7",
    CACHE_PUT_ALL: "#1BCDD1",
    CACHE_REMOVE_ALL: "#23BFAA",
    CACHE_INVOKE: "#F5A52A",
    CACHE_INVOKE_ALL: "#fd7e14",
    CACHE_PUT_ALL_CONFLICT: "#5289A4",
    CACHE_REMOVE_ALL_CONFLICT: "#17807E",
    CACHE_LOCK: "#FAA586",
    CACHE_LOAD: "#0050b3",
    CACHE_LOAD_ALL: "#3366cc",
    CACHE_LOAD_CACHE: "#5c7cfa",
    CACHE_WRITE: "#2f9e44",
    CACHE_WRITE_ALL: "#37b24d",
    CACHE_DELETE: "#d6336c",
    CACHE_DELETE_ALL: "#e8590c"
};

const searchCachesSelect = $('#searchCaches');
const searchNodesSelect = $('#searchNodes');

var opsCountPerType = {};
var storeOpsCountPerType = {};

function drawCacheCharts() {
    $("#operationsCharts").empty();
    $("#operationsCharts").append('<div id="cacheOperationsCharts"><h2>Cache operations</h2></div>');
    $("#operationsCharts").append('<div id="storeOperationsCharts"><h2 class="mt-5">Cache store operations</h2></div>');

    var cacheOperationsCharts = $("#cacheOperationsCharts");
    var storeOperationsCharts = $("#storeOperationsCharts");

    drawOperationCharts(cacheOperationsCharts, CACHE_OPERATIONS, CACHE_OPERATIONS_READABLE, opsCountPerType);

    drawBarChart(cacheOperationsCharts, "operationBarChart", CACHE_OPERATIONS, opsCountPerType, 'Distribution of count operations by type');

    drawOperationCharts(storeOperationsCharts, CACHE_STORE_OPERATIONS, CACHE_STORE_OPERATIONS_READABLE, storeOpsCountPerType);

    drawBarChart(storeOperationsCharts, "storeOperationBarChart", CACHE_STORE_OPERATIONS, storeOpsCountPerType, 'Distribution of cache store operations by type');
}

function prepareCacheDatasets(opName, countByType) {
    var cacheId = searchCachesSelect.val();
    var nodeId = searchNodesSelect.val();

    var datasets = [];

    var cacheOps = REPORT_DATA.cacheOps[nodeId] === undefined ? undefined : REPORT_DATA.cacheOps[nodeId][cacheId];

    if (cacheOps === undefined)
        return datasets;

    var datasetData = [];

    $.each(cacheOps[opName], function (k, arr) {
        datasetData.push({t: parseInt(arr[0]), y: arr[1]});

        countByType[opName] += arr[1];
    });

    sortByKeyAsc(datasetData, "t");

    var dataset = {
        data: datasetData,
        label: "Count of " + opName,
        lineTension: 0,
        backgroundColor: 'transparent',
        borderWidth: 2,
        pointRadius: 1,
        borderColor: CACHE_OPERATIONS_COLORS[opName],
        pointBackgroundColor: CACHE_OPERATIONS_COLORS[opName],
    };

    datasets.push(dataset);

    return datasets;
}

function drawOperationCharts(container, operations, readableNames, countPerType) {
    $.each(operations, function (k, opName) {
        countPerType[opName] = 0;

        var chartId = opName + "OperationChart";

        container.append('<canvas class="my-4" id="' + chartId + '" height="120"/>');

        new Chart(document.getElementById(chartId), {
            type: 'line',
            data: {
                datasets: prepareCacheDatasets(opName, countPerType)
            },
            options: {
                scales: {
                    xAxes: [{
                        type: 'time',
                        time: {
                            displayFormats: {
                                'millisecond': 'HH:mm:ss',
                                'second': 'HH:mm:ss',
                                'minute': 'HH:mm:ss',
                                'hour': 'HH:mm'
                            }
                        },
                        scaleLabel: {
                            display: true,
                            labelString: 'Date'
                        }
                    }],
                    yAxes: [{
                        display: true,
                        scaleLabel: {
                            display: true,
                            labelString: 'Count'
                        },
                        ticks: {
                            suggestedMin: 0,
                            suggestedMax: 10
                        }
                    }]
                },
                legend: {
                    display: true
                },
                title: {
                    display: true,
                    text: "Count of [" + readableNames[k] + "]",
                    fontSize: 20
                },
                animation: false
            }
        })
    });
}

function drawBarChart(container, chartId, labels, countPerType, title) {
    var html = '<canvas class="my-4" id="' + chartId + '" height="90"/>';

    var header = container.children("h2").first();

    header.after(html);

    var data = [];
    var colors = [];

    $.each(labels, function (k, opName) {
        data[k] = countPerType[opName];
        colors[k] = CACHE_OPERATIONS_COLORS[opName];
    });

    new Chart(document.getElementById(chartId), {
        type: 'bar',
        data: {
            datasets: [{
                data: data,
                backgroundColor: colors,
                label: 'Total count',
            }],
            labels: labels
        },
        options: {
            scales: {
                yAxes: [{
                    display: true,
                    scaleLabel: {
                        display: true,
                        labelString: 'Count'
                    },
                    ticks: {
                        suggestedMin: 0,
                        suggestedMax: 10
                    }
                }]
            },
            legend: {
                display: false
            },
            title: {
                display: true,
                text: title,
                fontSize: 20
            },
            animation: false
        }
    });
}

buildSelectCaches(searchCachesSelect, drawCacheCharts);
buildSelectNodes(searchNodesSelect, drawCacheCharts);

drawCacheCharts();
