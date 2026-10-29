-- Probe (not TPC-H): p01 plus Q5's shuffle of lineitem by l_orderkey (rows written to and read from the
-- shuffle before the hash).
select count(*) as n, sum(hash(l_orderkey, l_suppkey, l_extendedprice, l_discount)) as h
from (select /*+ REPARTITION(l_orderkey) */ l_orderkey, l_suppkey, l_extendedprice, l_discount from lineitem) t
