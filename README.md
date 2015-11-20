# StormOnEdge

Distributed Stream Processing Systems are typically deployed within a single data center in order to achieve high performance and low-latency computation. The data streams analyzed by such systems are expected to be available in the same data center. Either the data streams are generated within the data center (e.g., logs, transactions, user clicks) or they are aggregated by external systems from various sources and buffered into the data center for processing (e.g., IoT, sensor data, traffic information).

The data center approach for stream processing analytics fits the requirements of the majority of the applications that exists today. However, for latency sensitive applications, such as real-time decision-making, which relies on analyzing geographically distributed data streams, a data center approach might not be sufficient. Aggregating data streams incurs high overheads in terms of latency and bandwidth consumption in addition to the overhead of sending the analysis outcomes back to where an action needs to be taken.

We propose a new stream processing architecture for efficiently analyzing geographically distributed data streams. Our approach utilizes emerging distributed virtualized environments, such as Mobile Edge Clouds, to extend stream processing systems outside the data center in order to push critical parts of the analysis closer to the data sources. This will enable real-time applications to respond faster to geographically distributed events.

In order to realize our approach, we have implemented a geo-aware scheduler plugin for the Apache Storm stream processing system. The scheduler takes as an input a Storm topology (Figure 1) to be scheduled and executed on the available datacenter/edge Cloud resources. The scheduler enables the developers of the topology to annotate groups of stream processing elements that can be replicated and pushed outside of the data center to an Edge Cloud with close proximity to the stream source. In order to operate, the scheduler requires knowledge of the available datacenter/edge Clouds, data stream types available at each Cloud, and latencies among clouds.

![Figure 1](https://www.sics.se/~ahmad/fig/Topology.png "A typical Storm topology. The red rectangles indicate groups of stream processing elements that can be replicated and pushed to an Edge Cloud by the geo-aware scheduler.")

# Examples
An example of a topology to use this scheduler can be found at https://github.com/Telolets/StormOnEdge-Example

GeoAwareScheduler is based on Storm CustomScheduler plug-in interface. To use this scheduler, put the compiled jar file into Storm library ($STORM_BASE/lib) folder.

#Prerequisites

To make sure Nimbus call the correct Scheduler, user must put the GeoScheduler name as a custom scheduler. In our case, the class object name is "GeoAwareScheduler"

>storm.scheduler: "scheduler.GeoAwareScheduler"

By the design in this scheduler, there are some input needed to make the scheduler works as intended. The information needed are:

 - Location of the data sources
 - Quality between sites / clouds

There is also an output from GeoAwareScheduler that will be needed for ZoneGrouping at the runtime

- ZoneGrouping Info

This part can be easily modified by implementing three interfaces: CloudInfo, SourceInfo, and ZGConnector. In this version, all input and output information are done by reading or writing to files. All of the information below intended for the current version.

##Storm.yaml file

Storm finds the location path of three files explained above by reading the key-value in Storm configuration file (**Storm.yaml**).

**Nimbus Node**

> geoAwareScheduler.in-SourceInfo: "/home/user/Scheduler-SpoutCloudsPair.txt"

> geoAwareScheduler.in-CloudInfo: "/home/user/Scheduler-LatencyMatrix.txt"

> geoAwareScheduler.out-ZGConnector: "/home/user/PairSupervisorTasks.txt"

Example of both files can be seen in "data" folder. Both file are called every time the scheduler is called (periodically), so it is possible to modify the information when the Storm is running.


**Supervisor Node**

Every nodes that run Supervisor instances need to put information about their cloud location. 

> supervisor.scheduler.meta:

> name: "SUPERVISOR_1"

> cloud-name: "CLOUD_A"

Group of Supervisors that located in the same data-center / cloud should have same value for 'cloud_name'
