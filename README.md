# Kern
This repository contains a subproject of Kabina - Kern minibus dispatcher, which finds the optimal assignment plan for trip requests and available buses. This dispatcher can serve several hundred thousand passengers per hour while running on a regular PC. 

Kern dispatcher consists of three main components:
* **solver**, which allocates passengers to free cabs ([Hungarian](https://en.wikipedia.org/wiki/Hungarian_algorithm) and [Least Cost Method](https://www.educationlessons.co.in/notes/least-cost-method) are used)
* **pool finder** (multithreaded, linearly scalable, written in C) to assign several customers to one bus and create routes with several stops, 5+ passengers with 10+ stops are feasible (currently limited to 4 passengers) with finder based on [dynamic programming](https://en.wikipedia.org/wiki/Dynamic_programming) principles. This module is available also in Rust, Java and C#.
* **route extender** to assign customers to matching routes, including non-perfect matching (multithreaded)

## Other Kabina subprojects:
The idea behind Kabina is to provide an enabler (a software skeleton, testing framework and RestAPI proposal) for a minibus service that can assign 20+ passengers to one cab (minibus) per hour, thus reducing the cost of the driver per passenger, among other benefits. Such extended minibus service would allow for the shift to sustainable transport, it might be cost-competitive with the public transport while providing better service quality including shorter travel time.
The following accompanying components have been developed:

* [Kapir](https://gitlab.com/kabina/kapir): Rest API responsible for receiving requests, share statuses and store them in a database
* [Kapi](https://gitlab.com/kabina/kapi/client): Rest API client simulator, which emulates real users and helps test the dispatcher
* [Kim](https://gitlab.com/kabina/kim): client simulator, which emulates real users and helps test the dispatcher, direct access to the database
* [Kabina](https://gitlab.com/kabina/kabina): mobile application for minibus customers 
* [Kab](https://gitlab.com/kabina/kab): mobile application for minibus drivers
* [Kavla](https://gitlab.com/kabina/kavla): mobile application for presenting current routes on stops 
* [Kaut](https://gitlab.com/kabina/kaut): a panel in a cab for authentication, adding a passenger to an existing route or creating a new one when cab is free
* [Kaboot](https://gitlab.com/kabina/kaboot): alternative dispatcher with RestAPI and clients written in Java

There are still a few components missing that need to be added to make it a market-ready solution - billing among others. 

See here to find more: https://gitlab.com/kabina/kabina/-/blob/master/minibuses.pdf

## Prerequisites:
* MySQL or PostgreSQL (the latter currently not maintained, separate branch)
* C compiler (optional)
* Rust compiler

## How to install and run
See [readme](HOWTORUN.md) how to run all Kabina components in a simulation. Have a look at a summary on YouTube: https://www.youtube.com/watch?v=-CumrqlN33U 

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

### An example of an actual route saved in database

| id	| from_stand | to_stand |	distance | passengers |	status| started |	completed | reserve |	route_id | place
|-|-|-|-|-|-|-|-|-|-|-
| 8| 	2861| 	2078|	1|	1| 	6| 	2022-08-24 22:38:30	|2022-08-24 22:39:30	| 1| 	2|	0
| 9|	2078|	2176|	3|	2|	6|	2022-08-24 22:40:30|	2022-08-24 22:43:30|	1|	2|	1
|10|	2176|	2698|	3|	3|	6|	2022-08-24 22:44:30|	2022-08-24 22:47:30|	1|	2|	2
|11|	2698|	3127|	1|	4|	6|	2022-08-24 22:48:31|	2022-08-24 22:49:31|	0|	2|	3
|101|	3127|	2081|	2|	5|	6|	2022-08-24 22:50:31|	2022-08-24 22:52:31|	0|	2|	4
|12|	2081|	2863|	4|	4|	6|	2022-08-24 22:53:31|	2022-08-24 22:57:31|	0|	2|	5
|13|	2863|	3130|	4|	3|	6|	2022-08-24 22:58:31|	2022-08-24 23:02:31|	0|	2|	6
|102|	3130|	2179|	3|	2|	6|	2022-08-24 23:03:31|	2022-08-24 23:06:31|	0|	2|	7
|14|	2179|	2701|	4|	1|	6|	2022-08-24 23:07:31|	2022-08-24 23:11:31|	0|	2|	8

## Current work in Kern
* assigning cab while it is on a route's last leg.
* an order serviced by two cabs (cab change) 

## Future work
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

## Helpful SQL queries

1. Orders' statuses
`select status, count(*) from taxi_order group by status`

2. Legs of routes with specific number of passengers 
`select passengers,count(*) from leg group by passengers`

3. Number of orders in routes
`SELECT order_count, COUNT(*) AS route_count from
(SELECT route.id, count(taxi_order.id) as order_count        
from route left join taxi_order on (route.id = taxi_order.route_id) group by route.id)
AS aa GROUP BY order_count`

4. Average requested distance
`select avg(distance) from taxi_order`

5. Total wait (pick-up) time 
  `select sum(started-received) from taxi_order where received is not null and started is not null`

6. Total duration of all requests
  `select sum(completed-started) from taxi_order where completed is not null and started is not null`

7. Total distance driven
  `select sum(completed-started) from leg where completed is not null and started is not null`

8. Number of customers in pool
  `select in_pool,count(*) from taxi_order group by in_pool`

9. Number of customers in pool - distribution
  `SELECT order_count, COUNT(*) AS route_count from (SELECT route.id, count(taxi_order.id) as order_count from route left join taxi_order on (route.id = taxi_order.route_id) group by route.id) AS aa GROUP BY order_count`

10. Number of customers in legs - distribution
  `select passengers,count(*) from leg group by passengers`

11. Average pool size
`SELECT sum(order_count*route_count)/sum(route_count) FROM (SELECT order_count, COUNT(*) AS route_count from
(SELECT route.id, count(taxi_order.id) as order_count FROM route left join taxi_order on (route.id = taxi_order.route_id) group by route.id) AS aa GROUP BY order_count) as counts`
12. Average passenger count in legs
 `select sum(passengers*pass_count)/sum(pass_count) from 
 (select passengers,count(*) as pass_count from leg group by passengers) as average`
13. Total number of customers rejected
  `select count(*) from taxi_order where status=3`

14. Total number of customers with constraints violation (caused e.g. by RestAPI delays)
  `grep -c too customers.log`

15. Total cabs used
  `select count(distinct(cab_id)) from taxi_order`

## Copyright notice

Copyright 2025 Bogusz Jelinski

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
April 2025
Mo i Rana

bogusz.jelinski (at) g m a i l