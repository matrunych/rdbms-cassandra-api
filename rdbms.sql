CREATE DATABASE books;

CREATE TABLE product
(
    id       varchar(20) primary key,
    parent   text,
    title    text,
    category VARCHAR(20)
);

CREATE TABLE review
(
    id                varchar(20) primary key,
    marketplace       varchar(20),
    customer_id       varchar(20),
    product_id        varchar(20),
    star_rating       int,
    helpful_votes     int,
    total_votes       int,
    vine              boolean,
    verified_purchase boolean,
    headline          text,
    body              text,
    review_date       date,
    foreign key (product_id) references product (id)
);


CREATE TABLE reviews_data
(
    marketplace       VARCHAR(20),
    customer_id       VARCHAR(20),
    review_id         VARCHAR(20),
    product_id        VARCHAR(20),
    product_parent    text,
    product_title     text,
    product_category  VARCHAR(20),
    star_rating       INT,
    helpful_votes     INT,
    total_votes       INT,
    vine              BOOLEAN,
    verified_purchase BOOLEAN,
    review_headline   TEXT,
    review_body       TEXT,
    review_date       DATE
);

COPY reviews_data FROM 'data.tsv' DELIMITER E'\t'

INSERT INTO product (id, parent, title, category)
    (SELECT DISTINCT product_id, product_parent, product_title, product_category FROM reviews_data)

INSERT INTO review (id, marketplace, customer_id, product_id, star_rating, helpful_votes, total_votes,
                    vine, verified_purchase, headline, body, review_date)
    (SELECT review_id,
            marketplace,
            customer_id,
            product_id,
            star_rating,
            helpful_votes,
            total_votes,
            vine,
            verified_purchase,
            review_headline,
            review_body,
            review_date
     FROM reviews_data)