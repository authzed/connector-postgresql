CREATE TABLE customers(
      customer_id INT GENERATED ALWAYS AS IDENTITY,
      customer_name VARCHAR(255) NOT NULL,
      PRIMARY KEY(customer_id, customer_name)
);

CREATE TABLE contacts(
     contact_id INT GENERATED ALWAYS AS IDENTITY,
     customer_id INT,
     customer_name VARCHAR(255) NOT NULL,
     contact_name VARCHAR(255) NOT NULL,
     phone VARCHAR(15),
     email VARCHAR(100),
     PRIMARY KEY(contact_id),
     CONSTRAINT fk_customer
         FOREIGN KEY(customer_id,customer_name)
             REFERENCES customers(customer_id,customer_name)
             ON DELETE CASCADE
);

INSERT INTO customers(customer_name)
VALUES('BigCo'),
('SmallFry');

INSERT INTO contacts(customer_id, customer_name, contact_name, phone, email)
VALUES(1,'BigCo', 'John Doe','(408)-111-1234','john.doe@bigco.dev'),
(1,'BigCo','Jane Doe','(408)-111-1235','jane.doe@bigco.dev'),
(2,'SmallFry','Jeshk Doe','(408)-222-1234','jeshk.doe@smallfry.dev');

CREATE TABLE article (
     id SERIAL PRIMARY KEY,
     title TEXT
);

CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    tag_value TEXT
);

CREATE TABLE article_tag (
     article_id INT,
     tag_id INT,
     PRIMARY KEY (article_id, tag_id),
     CONSTRAINT fk_article FOREIGN KEY(article_id) REFERENCES article(id),
     CONSTRAINT fk_tag FOREIGN KEY(tag_id) REFERENCES tags(id)
);

INSERT INTO article(title)
VALUES('Dune'), ('Lies of Lock Lamora');

INSERT INTO tags(tag_value)
VALUES('scifi'), ('fantasy');

INSERT INTO article_tag(article_id,tag_id)
VALUES(1,1), (1,2), (2,2);