CREATE TABLE Place
(
	place_id       INT       NOT NULL,
	place_city     CHAR(50)  NOT NULL,
	place_country  CHAR(50)  NOT NULL
);

CREATE TABLE Industry 
(  
   ind_id        INT       NOT NULL,
   ind_1         CHAR(100) NULL,
   ind_2         CHAR(100) NULL,
   ind_3         CHAR(100) NULL
);

CREATE TABLE Company
(
   com_id          INT        NOT NULL,
   com_name        CHAR(100)  NOT NULL,
   com_valuation   CHAR(10)   NOT NULL,
   com_joined      DATE       NOT NULL,
   place_id        INT        NOT NULL,
   ind_id          INT        NOT NULL
);


-- -------------------
-- Define primary keys
-- -------------------
ALTER TABLE Place ADD PRIMARY KEY (place_id);
ALTER TABLE Industry ADD PRIMARY KEY (ind_id);
ALTER TABLE Company ADD PRIMARY KEY (com_id);


-- -------------------
-- Define foreign keys
-- -------------------
ALTER TABLE Company ADD CONSTRAINT FK_Company_Place FOREIGN KEY (place_id) REFERENCES Place (place_id);
ALTER TABLE Company ADD CONSTRAINT FK_Company_Industry FOREIGN KEY (ind_id) REFERENCES Industry (ind_id);
