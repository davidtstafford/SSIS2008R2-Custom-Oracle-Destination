# SSIS2008R2-Custom-Oracle-Destination
> Custom destination component to quickly write to oracle using their ODP.net driver

SSIS - Oracle Destination via ODP.Net (Custom Destination Component)

Author: David Stafford

SSIS 2008 R2 Solution.

I have created this custom destination component to allow ssis to write directly to an oracle table via the ODP.NET driver. This avoids the poor performance of using the ADO.NET Destination component with odp.net, which is incredibly slow.


Usage:

Use instead of ADO.NET destination.
Select ODP connection 
Variables:
- Batch Size (default 1000)
- Table name (destination table)
- Perform as transaction (True: Commit at end.. False: Commit for each batch size)
- Partition Name (If inserting into a partition table, this can be supplied to allow oracle to identify only one affect partition. Useful if running truncates on other partitions)

> Please Note

I created this code back in 2012 when SSIS 2008 R2 was the latest version.  I do not keep this code up to date, but it could be useful refernce.
