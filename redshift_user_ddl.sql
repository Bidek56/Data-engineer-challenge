create table users(
    vendorId                 integer      not null encode mostly16,
    firstName                varchar(50)  not null,
    lastName                 varchar(50)  not null,
    vendorIsActve            char(1),
    vendorLastActiveDt       date encode delta,
    practice_location        varchar(50) encode text,
    specialty                varchar(50) encode text,
    userTypeClassification   varchar(50) encode text,
    id                       integer      not null encode mostly16,
    practiceId               integer      not null encode mostly16,
    classification           varchar(50)  encode text,
    platformRegisteredOn     varchar(50)  encode text,
    lastActiveDt             date encode delta
)
diststyle key distkey(lastName)
compound sortkey(lastName, firstName );


select firstName, lastName, lastActiveDt, vendorLastActiveDt, vendorIsActve
from  users
where ( lastActiveDt >= dateadd(day,-30,getdate()) or  vendorLastActiveDt >= dateadd(day,-30,getdate()) )
