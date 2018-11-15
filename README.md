# JMeterJDBCSamplerWithOutTimeOut
A JMeterJDBCSampler without the timeout set when executing a query

Typically useful to test queries against Hive or Phoenix as their drivers doesn't implement the query timeout.

November 2018:
I need to update the code but I had to the same with JMeter 5 source code and the Hive JDBC driver to connect to a Spark 2.3 cluster.
