-- Probe (not TPC-H): the full lineitem stream of Q5's four columns; hash() is never pushed, so every row
-- reaches Spark and only a one-row partial aggregate follows the scan.
select count(*) as n, sum(hash(l_orderkey, l_suppkey, l_extendedprice, l_discount)) as h
from lineitem
