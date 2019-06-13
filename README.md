# exasol-dawa-udfs

## Overview

 ## Quick start


 1. Create Schema

Create a new schema eg. ETL_UDFS.

```
CREATE SCHEMA ETL_UDFS;
```



## Sources
### txid



```sql
insert into <table> select ETL_UDFS.dawa_txid(<txidfra>, <txidfra>)
```

example:

Initial extract

```sql
insert into <table> select ETL_UDFS.dawa_txid(NULL, NULL)
```

Incremental extract

```sql
insert into <table> select ETL_UDFS.dawa_txid(100, 1000)
```
