#/bin/bash
psql -h localhost -p 5432 -U kabina < kpis.sql
echo 'total number of customers who were not picked up'
grep 'vain' /Users/m91127/Rust/kim/kim.log | wc -l
echo 'total number of customers who were picked up late'
grep 'Cab came late' /Users/m91127/Rust/kim/kim.log | wc -l
echo 'total number of customers who were dropped off late'
grep 'Completed late' /Users/m91127/Rust/kim/kim.log | wc -l
