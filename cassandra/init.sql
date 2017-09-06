CREATE KEYSPACE bdr 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE bdr;

CREATE TABLE all_user_products (
user_id bigint,
product bigint,
PRIMARY KEY((user_id, product))
);

CREATE TABLE top_other_products_stream (
product bigint,
other_products list<bigint>,
PRIMARY KEY(product)
);

CREATE TABLE top_other_products_batch (
product bigint,
other_products list<bigint>,
PRIMARY KEY(product)
);

CREATE TABLE top_other_products_kappa (
product bigint,
other_products list<bigint>,
PRIMARY KEY(product)
);


CREATE TABLE users_interests (
    user_id bigint,
    product bigint,
    clicked_count counter,
    purchased_count counter,
    PRIMARY KEY ((user_id, product))
);

CREATE TABLE cf (
    user_id bigint PRIMARY KEY,
    recommended_products list<bigint>
);
