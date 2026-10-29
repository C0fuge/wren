-- Probe (not TPC-H): the full lineitem stream of all 16 columns (strings and dates included), as p01.
select count(*) as n, sum(hash(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice,
  l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct,
  l_shipmode, l_comment)) as h
from lineitem
