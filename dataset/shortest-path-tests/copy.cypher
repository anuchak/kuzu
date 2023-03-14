COPY Person FROM "ldbc-1/ldbc-1/person_0_0.csv" (DELIM="|", HEADER=true);
COPY knows FROM "ldbc-1/ldbc-1/person_knows_person_0_0.csv" (DELIM="|", HEADER=true);
