CREATE
KEYSPACE review_data WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE review_data.review_by_product
(
    marketplace       text,
    customer_id       text,
    review_id         text,
    product_id        text,
    product_parent    text,
    product_title     text,
    product_category  text,
    star_rating       int,
    helpful_votes     int,
    total_votes       int,
    vine              text,
    verified_purchase text,
    review_headline   text,
    review_body       text,
    PRIMARY KEY ((product_id), star_rating, review_id)
    )
WITH comment = 'Q1, Q2';

SELECT *
FROM review_data.review_by_customer
WHERE customer_id = '0316174947';
SELECT review_id
FROM review_data.review_by_product
WHERE product_id = '0521832357'
  AND star_rating = 5;

CREATE TABLE review_data.review_by_customer
(
    marketplace       text,
    customer_id       text,
    review_id         text,
    product_id        text,
    product_parent    text,
    product_title     text,
    product_category  text,
    star_rating       int,
    helpful_votes     int,
    total_votes       int,
    vine              text,
    verified_purchase text,
    review_headline   text,
    review_body       text,
    PRIMARY KEY ((customer_id), review_id)
    )
WITH comment = 'Q3';

SELECT *
FROM review_data.review_by_customer
WHERE customer_id = '43898203';

CREATE TABLE review_data.review_by_date
(
    marketplace       text,
    customer_id       text,
    review_id         text,
    product_id        text,
    product_parent    text,
    product_title     text,
    product_category  text,
    star_rating       int,
    helpful_votes     int,
    total_votes       int,
    vine              text,
    verified_purchase text,
    review_headline   text,
    review_body       text,
    review_date       date,
    PRIMARY KEY ((review_date), product_id, review_id)
    )
WITH comment = 'Q4';


CREATE TABLE review_data.review_by_date_verified
(
    marketplace       text,
    customer_id       text,
    review_id         text,
    product_id        text,
    product_parent    text,
    product_title     text,
    product_category  text,
    star_rating       int,
    helpful_votes     int,
    total_votes       int,
    vine              text,
    verified_purchase text,
    review_headline   text,
    review_body       text,
    review_date       date,
    PRIMARY KEY ((review_date), verified_purchase, product_id, customer_id)
    )
WITH comment = 'Q5';