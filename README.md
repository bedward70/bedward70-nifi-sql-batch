# bedward70-nifi-sql-batch

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Getting Help](#getting-help)
- [Installing](#installing)
- [License](#license)

## Features 

The general idea is using input json contents of FlowFiles to Batch Sql operations with parameterized values.

## Requirements
* Java 1.8
* [Apache NiFi](https://nifi.apache.org/)  v.1.14.0
It's additional NiFi Batch SQL processors.

I believe the project will be able to adapt to other versions Java and Apache NiFi.

## Getting Help
### For an inset example:
#### Table
    create table TEST (
        id integer not null, 
        val2 varchar(100), 
        constraint my_pk primary key (id)
    );
#### Insert SQL 
    insert into TEST (id, val2) VALUES (?,?);

#### NiFi FlowFile content
    [
        {"index": 1, "message" : "one"},
        {"index": 2, "message" : "two"}
    ]
#### Project NiFi processor
PutSQLBatchBedward70
#### Field Descriptions Json configuration
Mapping where each Json object describes one parameterized value:

1-st value for "id" column;
2-nd value for "val2" column.

    [
        {"jsonPath" : "$.index", "sqlType" : 4, "nameToResult" : "id"},
        {"jsonPath" : "$.message", "sqlType" : 12}
    ]
,where

"jsonPath" - JSONPath extracts a value from each Json object of context Json Array.

"sqlType" - integer sql code for SQL type for parameterized value. For example: [java.sql.Types](https://github.com/JetBrains/jdk8u_jdk/blob/master/src/share/classes/java/sql/Types.java)

"nameToResult" - optional parameter allows to add value to result Json with specified name;

#### Result field name for count values
    count

#### Result in table

    ID      VAL2
    --------------------
    1	one
    2	two

#### Result in one FlowFile with content

    [
        {"count":1,"id":1},
        {"count":1,"id":2}
    ]
,where

"count" - specified name contains impacted rows from each Json object of original context Json Array.

"id" - specified name by "Field Descriptions Json configuration" in "nameToResult" value.


### For a select example:
#### Table
    create table TEST (
        id integer not null, 
        val2 varchar(100), 
        constraint my_pk primary key (id)
    );
#### Table data
    ID      VAL2
    --------------------
    1	one
    2	two
#### Insert SQL
    select * from TEST where id = ?;
#### NiFi FlowFile content
    [
        {"count":1,"id":1},
        {"count":1,"id":2}
    ]
#### Project NiFi processor
ExecuteSQLBatchBedward70
#### Field Descriptions Json configuration
Mapping where each Json object describes one parameterized value:

1-st value for "id" column;

    [
        {"jsonPath" : "$.id", "sqlType" : 4}
    ]
,where

"jsonPath" - JSONPath extracts a value from each Json object of context Json Array.

"sqlType" - integer sql code for SQL type for parameterized value. For example: [java.sql.Types](https://github.com/JetBrains/jdk8u_jdk/blob/master/src/share/classes/java/sql/Types.java)


#### Result in two FlowFiles with AVRO contents
1-st

    [ 
        {"ID" : 1, "VAL2" : "one" } 
    ]
2-st

    [ 
      {"ID" : 2, "VAL2" : "two"} 
    ]

## Installing

1. Run `mvn clean install -Denforcer.skip=true`
2. Copy "nifi-bedward70-nifi-sql-batch-nar/target/nifi-bedward70-nifi-sql-batch-nar-1.0-SNAPSHOT.nar" file to "extensions" directory of NiFi.
3. Rerun NiFi.
4. You can use "templates/Test_ExecuteSQLBatchBedward70_and_PutSQLBatchBedward70.xml" NiFi template as an Example. Configure a real database before testing.

## License

Except as otherwise noted this software is licensed under the
[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.