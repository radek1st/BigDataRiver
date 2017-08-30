CREATE KEYSPACE bdr 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE bdr;

CREATE TABLE all_products (
product bigint,
other_product bigint,
count bigint,
PRIMARY KEY((product, other_product))
);

CREATE TABLE top_other_products (
product bigint,
other_products list<bigint>,
PRIMARY KEY(product)
);

CREATE TABLE users_interests (
    user_id bigint,
    product bigint,
    generic_cat int,
    specific_cat int,
    clicked_count counter,
    purchased_count counter,
    PRIMARY KEY ((user_id, product), generic_cat, specific_cat)
);

CREATE TABLE cf (
    user_id bigint PRIMARY KEY,
    recommended_products list<bigint>
);

CREATE TABLE top_generic (
   generic_cat int PRIMARY KEY,
   top_products list<bigint>
);

CREATE TABLE top_specific (
   specific_cat int PRIMARY KEY,
   top_products list<bigint>
);

COPY users_interests FROM '/data/train-small-cf-gs.csv' 
WITH DELIMITER=',' AND HEADER=FALSE;
