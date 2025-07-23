\echo 'Number of requests'
select count(*) from taxi_order;
\echo 'Number of requests per status'
select status,count(*) from taxi_order group by status;
\echo 'total cabs used'
select count(distinct(cab_id)) from taxi_order;
\echo 'avg requested distance'
select avg(distance) from taxi_order;
\echo 'total wait (pick-up) time'
select sum(started-received) from taxi_order where received is not null and started is not null;
\echo 'total duration of all requests'
select sum(completed-started) from taxi_order where completed is not null and started is not null;
\echo 'total distance driven'
select sum(completed-started) from leg where completed is not null and started is not null;
\echo 'number of customers in pool'
select in_pool,count(*) from taxi_order group by in_pool;
\echo 'number of customers in pool - distribution'
SELECT order_count, COUNT(*) AS route_count from (SELECT route.id, count(taxi_order.id) as order_count from route left join taxi_order on (route.id = taxi_order.route_id) group by route.id) AS aa GROUP BY order_count;
\echo 'number of customers in legs - distribution'
select passengers,count(*) from leg group by passengers;
\echo 'average pool size'
SELECT sum(order_count*route_count)/sum(route_count) FROM (SELECT order_count, COUNT(*) AS route_count from (SELECT route.id, count(taxi_order.id) as order_count from route left join taxi_order on (route.id = taxi_order.route_id) group by route.id) AS aa GROUP BY order_count) as counts;
\echo 'average passenger count in legs'
select sum(passengers*pass_count)/sum(pass_count) from (select passengers,count(*) as pass_count from leg group by passengers) as average;
\echo 'average number of legs per route'
select (select count(*) from leg)::float / (select count(*) from route)::float;
\echo 'total number of customers rejected'
select count(*) from taxi_order where status=3;
\echo 'avg detour: total duration / total requested distance (we measure distance with time)'
select (EXTRACT(epoch FROM (select sum(completed-started) from taxi_order where completed is not null and started is not null))/60)::float / (select sum(distance) from taxi_order where completed is not null and started is not null)::float;
\echo 'max scheduler time (seconds)' 
select * from stat where name='MaxSchedulerTime';
\echo 'average scheduler time (seconds)' 
select * from stat where name='AvgSchedulerTime';
\echo 'average pool finder input size (demand)'
select * from stat where name='AvgPoolDemandSize';
\echo 'average solver input size (demand)'
select * from stat where name='AvgSolverDemandSize';