# Mini-Sql-Engine
A Simple Mini-Sql-Engine to execute Queries Just like mysql

Drawbacks:
It doesn't take queries with '<' or '>' operator


Positives:
Multiple join implemented for any number of tables
Join takes O(n) time as implemented using hash
Aggregate functions like sum,max,min implemented
Distinct feature implemented

Run command just like in mysql
For eg
select table1.A from table1,table2,table3 where table1.B= table2.B and table1.A=table3.A

Command format:
python code.py query

