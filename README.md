# StormOnEdge

StormOnEdge is a master thesis project with the idea to run single Apache Storm sistem on multi-cloud platform. The example of a topology to use this scheduler can be checked in https://github.com/Telolets/StormOnEdge-Example

Geo-Scheduler is based on Storm CustomScheduler plug-in interface. To use this scheduler, put the compiled jar file into Storm library (lib/) folder.

##Prerequisites

###Storm.yaml file

In the current version, User needs to put some information needed by the Geo-scheduler manually by adding custom key-value data in **Storm.yaml** configuration file  

**Nimbus Node**

To make sure Nimbus call the correct Scheduler, user must put the GeoScheduler name as a custom scheduler. In our case, the class object name is "LocalGlobalGroupScheduler"

>storm.scheduler: "scheduler.LocalGlobalGroupScheduler"

Geo-scheduler needs to get additional information of the data sources location and connection quality between clouds. In the current version, these information are supplied from a text file. Path to the files are must be provided in the configuration file. Example of both files can be seen in "Additional file samples" folder. Both file are called every time the scheduler is called (periodically), so it is possible to modify the information when the Storm is running.

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


