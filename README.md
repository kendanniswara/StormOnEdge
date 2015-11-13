# StormOnEdge

Distributed Stream Processing Systems are typically deployed within a single data center in order to achieve high performance and low-latency computation. The data streams analyzed by such systems are expected to be available in the same data center. Either the data streams are generated within the data center (e.g., logs, transactions, user clicks) or they are aggregated by external systems from various sources and buffered into the data center for processing (e.g., IoT, sensor data, traffic information).

The data center approach for stream processing analytics fits the requirements of the majority of the applications that exists today. However, for latency sensitive applications, such as real-time decision-making, which relies on analyzing geographically distributed data streams, a data center approach might not be sufficient. Aggregating data streams incurs high overheads in terms of latency and bandwidth consumption in addition to the overhead of sending the analysis outcomes back to where an action needs to be taken.

We propose a new stream processing architecture for efficiently analyzing geographically distributed data streams. Our approach utilizes emerging distributed virtualized environments, such as Mobile Edge Clouds, to extend stream processing systems outside the data center in order to push critical parts of the analysis closer to the data sources. This will enable real-time applications to respond faster to geographically distributed events.

In order to realize our approach, we have implemented a geo-aware scheduler plugin for the Apache Storm stream processing system. The scheduler takes as an input a Storm topology (Figure 1) to be scheduled and executed on the available datacenter/edge Cloud resources. The scheduler enables the developers of the topology to annotate groups of stream processing elements that can be replicated and pushed outside of the data center to an Edge Cloud with close proximity to the stream source. In order to operate, the scheduler requires knowledge of the available datacenter/edge Clouds, data stream types available at each Cloud, and latencies among clouds.

![Figure 1](https://www.sics.se/~ahmad/fig/Topology.png "A typical Storm topology. The red rectangles indicate groups of stream processing elements that can be replicated and pushed to an Edge Cloud by the geo-aware scheduler.")

# Examples
An example of a topology to use this scheduler can be found at https://github.com/Telolets/StormOnEdge-Example

Geo-Scheduler is based on Storm CustomScheduler plug-in interface. To use this scheduler, put the compiled jar file into Storm library (lib/) folder.

#Prerequisites

##Storm.yaml file

In the current version, User needs to put some information needed by the Geo-scheduler manually by adding custom key-value data in **Storm.yaml** configuration file  

**Nimbus Node**

To make sure Nimbus call the correct Scheduler, user must put the GeoScheduler name as a custom scheduler. In our case, the class object name is "LocalGlobalGroupScheduler"

>storm.scheduler: "scheduler.LocalGlobalGroupScheduler"

Geo-scheduler needs to get additional information of the data sources location and connection quality between clouds. In the current version, these information are supplied from a text file. Path to the files are must be provided in the configuration file. Example of both files can be seen in "data" folder. Both file are called every time the scheduler is called (periodically), so it is possible to modify the information when the Storm is running.

> geoScheduler.sourceCloudList: "/home/user/Scheduler-SpoutCloudsPair.txt"

> geoScheduler.cloudInformation: "/home/user/Scheduler-LatencyMatrix.txt"

Geo-Scheduler generates 2 files: Scheduler report of the Task placements and extra file needed for the custom stream grouping (ZoneGrouping). The Scheduler report is optional and can be omitted if not needed. However, if user want to use ZoneGrouping in their Storm Topology then the later file is compulsory

> geoScheduler.out-SchedulerResult: "/home/user/SchedulerResult.csv"

> geoScheduler.out-ZoneGrouping: "/home/user/PairSupervisorTasks.txt"



**Supervisor Node**

Every nodes that run Supervisor instances need to add information about their cloud location. 

> supervisor.scheduler.meta:

> name: "SUPERVISOR_1"

> cloud-name: "CLOUD_A"

Group of Supervisors that located in the same data-center / cloud should have same value for 'cloud_name'


