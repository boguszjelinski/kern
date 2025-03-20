#/bin/bash
mysql -u kabina --password=kaboot --database=kabina < kpis-mysql.sql
echo 'total number of customers who were not picked up'
grep 'vain' ../../kim/kim.log | wc -l
echo 'total number of customers who were picked up late (longer than acceptable wait_time)'
grep 'Cab came late' ../../kim/kim.log | wc -l
echo 'total number of customers who were picked up late (> ETA)'
grep 'ETA' ../../kim/kim.log | wc -l
echo 'total number of customers who were dropped off late'
grep 'Completed late' ../../kim/kim.log | wc -l
