CREATE NODE TABLE Person(ID INT64, firstName STRING, lastName STRING, gender STRING, birthday DATE, creationDate TIMESTAMP, locationIP STRING, browserUsed STRING, PRIMARY KEY(ID));
CREATE REL TABLE knows(FROM Person TO Person, creationDate TIMESTAMP, MANY_MANY);
