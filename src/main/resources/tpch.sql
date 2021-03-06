DROP TABLE IF EXISTS PART CASCADE;
CREATE TABLE PART (

    PARTKEY		INTEGER PRIMARY KEY,
    NAME			VARCHAR(55) NOT NULL,
    MFGR			VARCHAR(25) NOT NULL,
    BRAND			VARCHAR(10) NOT NULL,
    TYPE			VARCHAR(25) NOT NULL,
    SIZE			INTEGER NOT NULL,
    CONTAINER		VARCHAR(10) NOT NULL,
    RETAILPRICE	DECIMAL(15, 2) NOT NULL,
    COMMENT		VARCHAR(23) NOT NULL,

    row_key        INTEGER NOT NULL

);

DROP TABLE IF EXISTS SUPPLIER CASCADE;
CREATE TABLE SUPPLIER (
    SUPPKEY		INTEGER PRIMARY KEY,
    NAME			VARCHAR(25) NOT NULL,
    ADDRESS		VARCHAR(40) NOT NULL,
    NATIONKEY		BIGINT NOT NULL, -- references NATIONKEY
    PHONE			VARCHAR(15) NOT NULL,
    ACCTBAL		DECIMAL(15, 2) NOT NULL,
    COMMENT		VARCHAR(101) NOT NULL,

    row_key        INTEGER NOT NULL
);

DROP TABLE IF EXISTS PARTSUPP CASCADE;
CREATE TABLE PARTSUPP (
    PARTKEY		BIGINT NOT NULL, -- references PARTKEY
    SUPPKEY		BIGINT NOT NULL, -- references SUPPKEY
    AVAILQTY		INTEGER NOT NULL,
    SUPPLYCOST	DECIMAL(15, 2) NOT NULL,
    COMMENT		VARCHAR(199) NOT NULL,

    row_key         INTEGER NOT NULL,

    PRIMARY KEY (PARTKEY, SUPPKEY)
);

DROP TABLE IF EXISTS CUSTOMER CASCADE;
CREATE TABLE CUSTOMER (
    CUSTKEY		INTEGER PRIMARY KEY,
    NAME			VARCHAR(25) NOT NULL,
    ADDRESS		VARCHAR(40) NOT NULL,
    NATIONKEY		BIGINT NOT NULL, -- references NATIONKEY
    PHONE			VARCHAR(15) NOT NULL,
    ACCTBAL		DECIMAL(15, 2) NOT NULL,
    MKTSEGMENT	VARCHAR(10) NOT NULL,
    COMMENT		VARCHAR(117) NOT NULL,

    row_key        INTEGER NOT NULL
);

DROP TABLE IF EXISTS ORDERS CASCADE;
CREATE TABLE ORDERS (
    ORDERKEY		INTEGER PRIMARY KEY,

CUSTKEY		BIGINT NOT NULL, -- references CUSTKEY
    ORDERSTATUS	VARCHAR(1) NOT NULL,
    TOTALPRICE	DECIMAL(15, 2) NOT NULL,
    ORDERDATE		DATE NOT NULL,
    ORDERPRIORITY	VARCHAR(15) NOT NULL,
    CLERK			VARCHAR(15) NOT NULL,
    SHIPPRIORITY	INTEGER NOT NULL,
    COMMENT		VARCHAR(79) NOT NULL,

    row_key         INTEGER NOT NULL
);

DROP TABLE IF EXISTS LINEITEM CASCADE;
CREATE TABLE LINEITEM (
    ORDERKEY		BIGINT NOT NULL, -- references ORDERKEY
    PARTKEY		BIGINT NOT NULL, -- references PARTKEY (compound fk to PARTSUPP)
    SUPPKEY		BIGINT NOT NULL, -- references SUPPKEY (compound fk to PARTSUPP)
    LINENUMBER	INTEGER NOT NULL,
    QUANTITY		DECIMAL(15, 2) NOT NULL,
    EXTENDEDPRICE	DECIMAL(15, 2) NOT NULL,
    DISCOUNT		DECIMAL(15, 2) NOT NULL,
    TAX			DECIMAL(15, 2) NOT NULL,
    RETURNFLAG	VARCHAR(1) NOT NULL,
    LINESTATUS	VARCHAR(1) NOT NULL,
    SHIPDATE		DATE NOT NULL,
    COMMITDATE	DATE NOT NULL,
    RECEIPTDATE	DATE NOT NULL,
    SHIPINSTRUCT	VARCHAR(25) NOT NULL,
    SHIPMODE		VARCHAR(10) NOT NULL,
    COMMENT		VARCHAR(44) NOT NULL,

    row_key         INTEGER NOT NULL,

    PRIMARY KEY (ORDERKEY, LINENUMBER)
);

DROP TABLE IF EXISTS NATION CASCADE;
CREATE TABLE NATION (
    NATIONKEY		INTEGER PRIMARY KEY,
    NAME			VARCHAR(25) NOT NULL,
    REGIONKEY		BIGINT NOT NULL,  -- references REGIONKEY
    COMMENT		VARCHAR(152) NOT NULL,

    row_key        INTEGER NOT NULL
);

DROP TABLE IF EXISTS REGION CASCADE;
CREATE TABLE REGION (
    REGIONKEY	INTEGER PRIMARY KEY,
    NAME		VARCHAR(25) NOT NULL,
    COMMENT	VARCHAR(152) NOT NULL,

    row_key     INTEGER NOT NULL
);
