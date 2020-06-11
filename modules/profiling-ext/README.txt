Apache Ignite Profiling Module
------------------------------

Apache Ignite Profiling module provides the cluster profiling tool.

Cluster nodes collect performance statistics to the profiling files placed under 'Ignite_work_directory/profiling/'.
Profiling files are used to build the report offline.

To collect statistics in runtime and to build the performance report follow:

1) Start profiling (use IgniteProfilingMBean JMX bean).
2) Collect workload statistics.
3) Stop profiling (use IgniteProfilingMBean JMX bean).
4) Collect profiling files from all nodes under an empty directory. For example:

    path_to_profiling_files
        ├── node-162c7147-fef8-4ea2-bd25-8653c41fc7fa.prf
        ├── node-7b8a7c5c-f3b7-46c3-90da-e66103c00001.prf
        └── node-faedc6c9-3542-4610-ae10-4ff7e0600000.prf

5) Place the profiling tool under Ignite home directory. For example:

    ${IGNITE_HOME}
        ├── benchmarks
        ├── bin
        ├── config
        ├── examples
        ├── libs
        ├── platforms
        ├── profiling
        │   ├── libs
        │   ├── ignite-profiling-ext-x.x.x.jar
        │   └── build-report.sh
        ├── LICENSE
        ├── MIGRATION_GUIDE.txt
        ├── NOTICE
        ├── README.txt
        └── RELEASE_NOTES.txt

    Run script

        profiling/build-report.sh path_to_profiling_files

    to build the performance report. The report will be created in the new directory under the profiling files path:

        path_to_profiling_files/report_yyyy-MM-dd_HH-mm-ss/

    Open 'report_yyyy-MM-dd_HH-mm-ss/index.html' in the browser to see the report.
