-- 
CREATE OR REPLACE FUNCTION mylsm_handler(internal)
RETURNS index_am_handler
AS '../../install/lib/mylsm.so'
LANGUAGE C;

-- Comparision functions are same as defined in src/backend/access/nbtree/nbtcompare.c
CREATE ACCESS METHOD mylsm TYPE INDEX HANDLER mylsm_handler;

-- operator family create for cross datatype oprerands and null values it is alter later in file
CREATE OPERATOR FAMILY integer_ops USING mylsm;

-- Operator class to compare int2 operands it use existing btree oprator function btint2cmp
-- operator class defines operators to be used index of a column

CREATE OPERATOR CLASS int2_ops DEFAULT
	FOR TYPE int2 USING mylsm FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint2cmp(int2,int2);

-- Operator class to compare int4 operands it use exicting btree oprator function btint4cmp
CREATE OPERATOR CLASS int4_ops DEFAULT
	FOR TYPE int4 USING mylsm FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint4cmp(int4,int4);
-- Operator class to compare int8 operands it use exicting btree oprator function btint8cmp
CREATE OPERATOR CLASS int8_ops DEFAULT
	FOR TYPE int8 USING mylsm FAMILY integer_ops AS
	OPERATOR 1  <,
	OPERATOR 2  <=,
	OPERATOR 3  =,
	OPERATOR 4  >=,
	OPERATOR 5  >,
	FUNCTION 1  btint8cmp(int8,int8);
-- Operator class family to compare int2 operands it use exicting btree oprator function btint2cmp
ALTER OPERATOR FAMILY integer_ops USING mylsm ADD
	OPERATOR 1  < (int2,int4),
	OPERATOR 1  < (int2,int8),
	OPERATOR 1  < (int4,int2),
	OPERATOR 1  < (int4,int8),
	OPERATOR 1  < (int8,int2),
	OPERATOR 1  < (int8,int4),

	OPERATOR 2  <= (int2,int4),
	OPERATOR 2  <= (int2,int8),
	OPERATOR 2  <= (int4,int2),
	OPERATOR 2  <= (int4,int8),
	OPERATOR 2  <= (int8,int2),
	OPERATOR 2  <= (int8,int4),

	OPERATOR 3  = (int2,int4),
	OPERATOR 3  = (int2,int8),
	OPERATOR 3  = (int4,int2),
	OPERATOR 3  = (int4,int8),
	OPERATOR 3  = (int8,int2),
	OPERATOR 3  = (int8,int4),

	OPERATOR 4  >= (int2,int4),
	OPERATOR 4  >= (int2,int8),
	OPERATOR 4  >= (int4,int2),
	OPERATOR 4  >= (int4,int8),
	OPERATOR 4  >= (int8,int2),
	OPERATOR 4  >= (int8,int4),

	OPERATOR 5  > (int2,int4),
	OPERATOR 5  > (int2,int8),
	OPERATOR 5  > (int4,int2),
	OPERATOR 5  > (int4,int8),
	OPERATOR 5  > (int8,int2),
	OPERATOR 5  > (int8,int4),

	FUNCTION 1(int2,int4)  btint24cmp(int2,int4),
	FUNCTION 1(int2,int8)  btint28cmp(int2,int8),
	FUNCTION 1(int4,int2)  btint42cmp(int4,int2),
	FUNCTION 1(int4,int8)  btint48cmp(int4,int8),
	FUNCTION 1(int8,int4)  btint84cmp(int8,int4),
	FUNCTION 1(int8,int2)  btint82cmp(int8,int2),

	FUNCTION 2(int2,int2)  btint2sortsupport(internal),
	FUNCTION 2(int4,int4)  btint4sortsupport(internal),
	FUNCTION 2(int8,int8)  btint8sortsupport(internal),

    FUNCTION 3(int2,int8)  in_range(int2,int2,int8,bool,bool),
    FUNCTION 3(int2,int4)  in_range(int2,int2,int4,bool,bool),
    FUNCTION 3(int2,int2)  in_range(int2,int2,int2,bool,bool),
    FUNCTION 3(int4,int8)  in_range(int4,int4,int8,bool,bool),
    FUNCTION 3(int4,int4)  in_range(int4,int4,int4,bool,bool),
    FUNCTION 3(int4,int2)  in_range(int4,int4,int2,bool,bool),
    FUNCTION 3(int8,int8)  in_range(int8,int8,int8,bool,bool),

    FUNCTION 4(int2,int2)  btequalimage(oid),
    FUNCTION 4(int4,int4)  btequalimage(oid),
    FUNCTION 4(int8,int8)  btequalimage(oid);
