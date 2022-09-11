# Kern
This repository contains a subproject of Kabina - Kern minibus dispatcher, which finds the optimal assignment plan for trip requests and available buses.

Kern dispatcher consists of three main components:
* **solver**, which allocates passengers to free cabs ([Hungarian](https://en.wikipedia.org/wiki/Hungarian_algorithm) and [Least Cost Method](https://www.educationlessons.co.in/notes/least-cost-method) are used)
* **pool finder** (multithreaded, linearly scalable, written in C) to assign several customers to one bus and create routes with several stops, 5+ passengers with 10+ stops are feasible with finder based on [dynamic programming](https://en.wikipedia.org/wiki/Dynamic_programming) principles. This module is available also in Rust, Java and C#.
* **route extender** to assign customers to matching routes (including non-perfect matching)  

## Other Kabina subprojects:
The idea behind Kabina is to provide an enabler (a software skeleton, testing framework and RestAPI proposal) for a minibus service that can assign 10+ passengers to one cab (minibus), thus reducing among others the cost of the driver per passenger. Such extended minibus service would allow for the shift to sustainable transport, it might be cost-competitive with the public transport while providing better service quality including shorter travel time.
The following accompanying components have been developed:

* Kapir: Rest API responsible for receiving requests, share statuses and store them in a database
* Kapi: Rest API client simulator, which emulates real users and helps test the dispatcher
* Kabina: mobile application for minibus customers 
* Kab: mobile application for minibus drivers
* Kavla: mobile application for presenting current routes on stops 
* Kaboot: alternative dispatcher with RestAPI and clients written in Java

There are still a few components missing that need to be added to make it a market-ready solution - billing among others. 

See here to find more: https://gitlab.com/kabina/kabina/-/blob/master/minibuses.pdf

## Prerequisites:
* PostgreSQL
* C compiler (optional)
* Rust compiler

## How to install and run

1) Compile the pool finder (optional, see use_ext_pool below) and make the library available for Rust compiler, an example for Mac OS:
```
cd pool
cc -c -Wno-implicit-function-declaration poold.c dynapool.c -w
ar -cvq libdynapool.a poold.o dynapool.o
sudo cp libdynapool.a /Library/Developer/CommandLineTools/SDKs/MacOSX11.1.sdk/usr/lib/
```
2) Compile the dispatcher 
```
cargo build --release 
```

3) Create DB schema
We assume that DB schema and user have been created beforehand, here 'kabina':
```
cd sql
psql -U kabina kabina < create.sql
psql -U kabina -c "COPY stop(id, no, name, latitude, longitude, bearing) FROM 'stops-Budapest-import.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';"
```
This will create example stop, cab and customer entities. 

 4) Edit config file <em>kern.toml</em>

| Parameter | Purpose
|----------|--------|----------|----------------------------------
| db_conn | database connection string - user, password, address, port, schema
| run_after | time difference in seconds between dispatcher executions
| max_assign_time | time in minutes after which orders expire
| max_solver_size | if demand and supply exceed the value LCM will be called to shrink the model
| max_legs | how many legs can a route have, used in route extender
| extend_margin | max ratio by which a leg can be extended (its distance), currently ignored 
| max_angle | max angle between consecutive stops; used to promote streight routes 
| cab_speed | average speed in km/h
| stop_wait | how many minutes it takes at a stop
| log_file  | log file location and name
| use_ext_pool | if external pool finder (C library) should be used
| thread_numb | how many threads should be used

Scheduler can be started with *target/release/kern*
Though nothing will happen until cabs will report their availability and customers will submit their trip requests, both via RestAPI. 

### Rest API 
There are four API implementations, only that written in Rust will be maintained:
[Kapir](https://gitlab.com/kabina/kapir): Rust (Actix)
[Kapi](https://gitlab.com/kabina/kapi): Go (two versions - Echo and Gin)
[Kaboot](https://gitlab.com/kabina/kaboot): Java (Spring Boot)
[Kore](https://gitlab.com/kabina/kore): C# (.Net Core)

Just build the Rust one with *cargo build --release* and run with *target/release/kapir*

### Rest API client simulators
There are two implementations, Go will be maintained:
[Kapi](https://gitlab.com/kabina/kapi/-/tree/main/client): Go
[Kaboot](https://gitlab.com/kabina/kaboot/-/tree/master/generators/src) Java (see [README.md](https://gitlab.com/kabina/kaboot/-/blob/master/README.md) how to run it)

*go build* will make a **kabina** executable that runs in two ways:
*./kabina* runs threads with customers
*./kabina cab* runs threads with cabs. You should run it first and wait a minute so that cabs manage to update their availability.

### How to rerun
One has to clean up some tables to run a simulation again:
```
update cab set status=2;
update stat set int_val=0;
delete from taxi_order;
delete from leg;
delete from route;
```
## How it works
### Core
* available buses (cabs) and incoming requests from customers are read from database
* requests that match routes currently executed (to be exact - their legs that still wait to be completed) get assigned to these routes
* pool discoverer checks if we can assign more customers than one to a cab without affecting badly duration of their trips. Each customer can choose their tolerance, or decide that a pool is not acceptable. Maximally four passengers can be assigned to one cab due to core's performance limitations. Pool discoverer produces pools with four, three and two customers. 
* Unassigned customers (without a pool) are sent to LCM pre-solver if the resulting model exceeds an assumed solver's limit. Solver produces better plans than LCM but time spent on finding optimal solutions, which theoretically means shorter overall wait time, causes longer ... wait time. We need a balance here.
* models reduced by LCM are sent to Hungarian (aka Munkres) solver.
* after all this effort 'routes' with 'legs' are created in the database, 'cab' and 'taxi_order' tables are updated - marked as 'assigned'. RestAPI clients receive this information - cabs begin to move, customers wait for notification that their cabs have reached their stands and can pick them up. Currently, the following statuses may be assigned to an order:
  - RECEIVED: sent by customer
  - ASSIGNED: assigned to a cab, a proposal sent to customer with time-of-arrival
  - ACCEPTED: plan accepted by a customer, waiting for the cab
  - CANCELLED: cancelled by a customer before assignment
  - REJECTED:  proposal rejected by customer
  - ABANDONED: cancelled after assignment but before PICKEDUP
  - REFUSED: no cab available
  - PICKEDUP: cab has arrived
  - COMPLETED: customer dropped off
  
### Cab
* wait for a route
* after having received a route - go to the first customer and follow 'legs' of the route.
* wait 1min after having reached a waypoint (stand) - time for customers to get the notification via RestAPI
* mark cab as FREE at the last stand

### Customer
* request a cab
* wait for an assignment - a proposal 
* do you like it ?
* wait for a cab
* take a trip
* mark the end (COMPLETED)

## Current work in kaboot
* faster sigle threaded route extender (single thread has its pros)

## Future work
* take cabs on last leg into account
* trip with a change of cab (better cab utilization)
* distance service based on data from the field
* charging plans & payment integration
* resistance to bizarre situations (customers interrupting trips, for example)
* extended tuning  

## KPIs
During runtime a few measures are gathered and stored in the database - see 'stat' table. It allows for 
tuning of the core:
- avg_lcm_size
- avg_lcm_time
- avg_model_size
- avg_order_assign_time	
- avg_order_complete_time	
- avg_order_pickup_time	
- avg_pool_time		
- avg_pool3_time		
- avg_pool4_time		
- avg_sheduler_time	
- avg_solver_size		
- avg_solver_time		
- max_lcm_size		
- max_lcm_time		
- max_model_size		
- max_pool_time		
- max_pool3_time		
- max_pool4_time		
- max_sheduler_time	
- max_solver_size		
- max_solver_time		
- total_lcm_used		
- total_pickup_distance

## Copyright notice

Copyright 2022 Bogusz Jelinski

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
#
Bogusz Jelinski    
January 2022  
Mo i Rana

bogusz.jelinski (at) g m a i l