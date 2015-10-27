# StormOnEdge

StormOnEdge is a master thesis project with the idea to run single Apache Storm sistem on multi-cloud platform. The example of a topology to use this scheduler can be checked in https://github.com/Telolets/StormOnEdge-Test

Geo-Scheduler is based on Storm CustomScheduler plug-in interface. To use this scheduler, put the compiled jar file into Storm library (lib/) folder.

##Prerequisites

###Storm.yaml file

In the current version, User needs to put some information needed by the Geo-Scheduler manually by adding custom key-value data in Storm.yaml configuration file  

**Nimbus Node**

To make sure Nimbus call the correct Scheduler, user must put the GeoScheduler as a custom scheduler

>storm.scheduler: "scheduler.LocalGlobalGroupScheduler"

Geo-Scheduler needs to get additional information of where the data sources are located and connection quality between clouds. In the current version, these information are supplied from a text file. Path to the files are must be provided in the configuration file.

> geoScheduler.sourceCloudList: "/home/kend/fromSICSCloud/Scheduler-SpoutCloudsPair.txt"
> geoScheduler.cloudInformation: "/home/kend/fromSICSCloud/Scheduler-LatencyMatrix.txt"

Geo-Scehduler generates 2 files: Scheduler report of the Task placements and extra file needed for the custom stream grouping (ZoneGrouping). The Scheduler report is optional and can be omitted if not needed. However, if user want to use ZoneGrouping in their Storm Topology then the later file is compulsory

>geoScheduler.out-SchedulerResult: "/home/kend/SchedulerResult.csv"
>geoScheduler.out-ZoneGrouping: "/home/kend/fromSICSCloud/PairSupervisorTasks.txt"



**Supervisor Node**

Every nodes that run Supervisor instances need to add information about their cloud location. 

> supervisor.scheduler.meta:

> name: "SUPERVISOR_NAME"

> cloud-name: "CLOUD_NAME"

Group of Supervisors that located in the same datacenter or cloud should have same value for 'cloud_name' 
