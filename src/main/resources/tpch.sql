DROP TABLE IF EXISTS PART CASCADE;
CREATE TABLE PART (

    P_PARTKEY		INTEGER PRIMARY KEY,
    P_NAME			VARCHAR(55) NOT NULL,
    P_MFGR			CHAR(25) NOT NULL,
    P_BRAND			CHAR(10) NOT NULL,
    P_TYPE			VARCHAR(25) NOT NULL,
    P_SIZE			INTEGER NOT NULL,
    P_CONTAINER		CHAR(10) NOT NULL,
    P_RETAILPRICE	DECIMAL(15, 2) NOT NULL,
    P_COMMENT		VARCHAR(23) NOT NULL,

    row_key        INTEGER NOT NULL

);

DROP TABLE IF EXISTS SUPPLIER CASCADE;
CREATE TABLE SUPPLIER (
    S_SUPPKEY		INTEGER PRIMARY KEY,
    S_NAME			CHAR(25) NOT NULL,
    S_ADDRESS		VARCHAR(40) NOT NULL,
    S_NATIONKEY		BIGINT NOT NULL, -- references N_NATIONKEY
    S_PHONE			CHAR(15) NOT NULL,
    S_ACCTBAL		DECIMAL(15, 2) NOT NULL,
    S_COMMENT		VARCHAR(101) NOT NULL,

    row_key        INTEGER NOT NULL
);

DROP TABLE IF EXISTS PARTSUPP CASCADE;
CREATE TABLE PARTSUPP (
    PS_PARTKEY		BIGINT NOT NULL, -- references P_PARTKEY
    PS_SUPPKEY		BIGINT NOT NULL, -- references S_SUPPKEY
    PS_AVAILQTY		INTEGER NOT NULL,
    PS_SUPPLYCOST	DECIMAL(15, 2) NOT NULL,
    PS_COMMENT		VARCHAR(199) NOT NULL,

    row_key         INTEGER NOT NULL,

    PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
);

DROP TABLE IF EXISTS CUSTOMER CASCADE;
CREATE TABLE CUSTOMER (
    C_CUSTKEY		INTEGER PRIMARY KEY,
    C_NAME			VARCHAR(25) NOT NULL,
    C_ADDRESS		VARCHAR(40) NOT NULL,
    C_NATIONKEY		BIGINT NOT NULL, -- references N_NATIONKEY
    C_PHONE			CHAR(15) NOT NULL,
    C_ACCTBAL		DECIMAL(15, 2) NOT NULL,
    C_MKTSEGMENT	CHAR(10) NOT NULL,
    C_COMMENT		VARCHAR(117) NOT NULL,

    row_key        INTEGER NOT NULL
);

DROP TABLE IF EXISTS ORDERS CASCADE;
CREATE TABLE ORDERS (
    O_ORDERKEY		INTEGER PRIMARY KEY,

O_CUSTKEY		BIGINT NOT NULL, -- references C_CUSTKEY
    O_ORDERSTATUS	CHAR(1) NOT NULL,
    O_TOTALPRICE	DECIMAL(15, 2) NOT NULL,
    O_ORDERDATE		DATE NOT NULL,
    O_ORDERPRIORITY	CHAR(15) NOT NULL,
    O_CLERK			CHAR(15) NOT NULL,
    O_SHIPPRIORITY	INTEGER NOT NULL,
    O_COMMENT		VARCHAR(79) NOT NULL,

    row_key         INTEGER NOT NULL
);

DROP TABLE IF EXISTS LINEITEM CASCADE;
CREATE TABLE LINEITEM (
    L_ORDERKEY		BIGINT NOT NULL, -- references O_ORDERKEY
    L_PARTKEY		BIGINT NOT NULL, -- references P_PARTKEY (compound fk to PARTSUPP)
    L_SUPPKEY		BIGINT NOT NULL, -- references S_SUPPKEY (compound fk to PARTSUPP)
    L_LINENUMBER	INTEGER NOT NULL,
    L_QUANTITY		DECIMAL(15, 2) NOT NULL,
    L_EXTENDEDPRICE	DECIMAL(15, 2) NOT NULL,
    L_DISCOUNT		DECIMAL(15, 2) NOT NULL,
    L_TAX			DECIMAL(15, 2) NOT NULL,
    L_RETURNFLAG	CHAR(1) NOT NULL,
    L_LINESTATUS	CHAR(1) NOT NULL,
    L_SHIPDATE		DATE NOT NULL,
    L_COMMITDATE	DATE NOT NULL,
    L_RECEIPTDATE	DATE NOT NULL,
    L_SHIPINSTRUCT	CHAR(25) NOT NULL,
    L_SHIPMODE		CHAR(10) NOT NULL,
    L_COMMENT		VARCHAR(44) NOT NULL,

    row_key         INTEGER NOT NULL,

    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
);

DROP TABLE IF EXISTS NATION CASCADE;
CREATE TABLE NATION (
    N_NATIONKEY		INTEGER PRIMARY KEY,
    N_NAME			CHAR(25) NOT NULL,
    N_REGIONKEY		BIGINT NOT NULL,  -- references R_REGIONKEY
    N_COMMENT		VARCHAR(152) NOT NULL,

    row_key        INTEGER NOT NULL
);

DROP TABLE IF EXISTS REGION CASCADE;
CREATE TABLE REGION (
    R_REGIONKEY	INTEGER PRIMARY KEY,
    R_NAME		CHAR(25) NOT NULL,
    R_COMMENT	VARCHAR(152) NOT NULL,

    row_key     INTEGER NOT NULL
);