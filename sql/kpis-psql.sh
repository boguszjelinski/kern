#!/bin/bash
touch kpis.txt
touch cab_status.txt
touch order_status.txt
for i in {1..70}
do
  PGPASSWORD=kaboot psql -U kabina -w -d kabina -c 'select * from stat order by name' >> kpis.txt
  PGPASSWORD=kaboot psql -U kabina -w -d kabina -c 'select status, count(*) from cab group by status order by status' >> cab_status.txt
  PGPASSWORD=kaboot psql -U kabina -w -d kabina -c 'select status, count(*) from taxi_order group by status order by status' >> order_status.txt
  sleep 60
done