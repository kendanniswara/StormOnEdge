# StormOnEdge

StormOnEdge is a master thesis project with the idea to run single Apache Storm sistem on multi-cloud platform. The example of a topology to use this scheduler can be checked in https://github.com/Telolets/StormOnEdge-Test

##Prerequisites

###Storm.yaml file

In the current version, User needs to put some information needed by the Geo-Scheduler manually by adding custom key-value data in Storm.yaml configuration file  

**Nimbus Node**

To make sure Nimbus call the correct Scheduler, user must put the GeoScheduler as a custom scheduler

>storm.scheduler: "scheduler.LocalGlobalGroupScheduler"

Geo-Scheduler needs to get additional information of where the data sources are located and connection quality between clouds. In the current version, these information are supplied from a text file. Path to the files are also need to be provided in the configuration file

> geoScheduler.sourceCloudList: "/home/kend/fromSICSCloud/Scheduler-SpoutCloudsPair.txt"
> geoScheduler.cloudInformation: "/home/kend/fromSICSCloud/Scheduler-LatencyMatrix.txt"



**Supervisor Node**

Every nodes that run Supervisor instances need to add information about their cloud location. 

> supervisor.scheduler.meta:
>   name: "SUPERVISOR_NAME"
>   cloud-name: "CLOUD_NAME"

Group of Supervisors that located in the same datacenter or cloud should have same value 