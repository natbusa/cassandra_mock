# simpleSQL.py
#
# simple demo of using the parsing library to do simple-minded SQL parsing
# could be extended to include where clauses etc.
#
# Copyright (c) 2003,2016, Paul McGuire
#
from pyparsing import Literal, CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword, downcaseTokens

# numbers
E = CaselessLiteral("E")
arithSign = Word("+-", exact=1)

# integers
intNum = Combine(Optional(arithSign) + Word(nums) +
                 Optional(E + Optional("+") + Word(nums)))

# reals
realNum = Combine(Optional(arithSign) +
                  (Word(nums) + "." + Optional(Word(nums)) | ("." + Word(nums))) +
                  Optional(E + Optional(arithSign) + Word(nums)))

# identifier
ident = Word(alphas, alphanums + "_$").setName("identifier")

# define SQL tokens
SELECT = Keyword("select", caseless=True)
FROM = Keyword("from", caseless=True)
WHERE = Keyword("where", caseless=True)
LIMIT = Keyword("limit", caseless=True)
INSERT = Keyword("insert", caseless=True)
INTO = Keyword("into", caseless=True)
VALUES = Keyword("values", caseless=True)
UPDATE = Keyword("update", caseless=True)
SET = Keyword("set", caseless=True)
IF = Keyword("if", caseless=True)
NOT = Keyword("not", caseless=True)
EXISTS = Keyword("exists", caseless=True)
DELETE = Keyword("delete", caseless=True)
USE = Keyword("use", caseless=True)
CREATE = Keyword("create", caseless=True)
TABLE = Keyword("table", caseless=True)
PRIMARY = Keyword("primary", caseless=True)
KEY = Keyword("key", caseless=True)

# column names
columnName = ident.setName("column").addParseAction(downcaseTokens)
columnNameList = Group(delimitedList(columnName))

# table name
keyspaceName = ident.addParseAction(downcaseTokens).setName("keyspace")
tableName = Group(Optional(keyspaceName + ".") + ident.addParseAction(downcaseTokens)).setName("table")

# where clause
and_ = Keyword("and", caseless=True)
binop = oneOf("= eq != < > >= <= eq ne lt le gt ge", caseless=True)
Rval = realNum('real') | intNum('int') | quotedString('quoted')  # need to add support for alg expressions
RvalList = Group(delimitedList(Rval))

# where expression
whereExpression = Forward()
whereCondition = Group((columnName() + binop + Rval))
whereExpression <<= whereCondition + ZeroOrMore(and_ + whereExpression)

# set expression
setExpression = Forward()
setAssignment = Group((columnName() + "=" + Rval))
setExpression <<= setAssignment + ZeroOrMore("," + setExpression)

# if expression
ifConditionList = Forward()
ifCondition = Group((columnName() + "=" + Rval))
ifConditionList <<= ifCondition + ZeroOrMore(and_ + ifConditionList)

ifExpression = ((NOT + EXISTS) | EXISTS | ifConditionList)

# limit clause
limitExpr = (LIMIT + intNum)

# compositeKeyDefinition
lparen = Literal('(').suppress()
rparen = Literal(')').suppress()

compositeKeyDefinition = Group(columnName) | \
                         Group(lparen + columnNameList + rparen) | \
                         Group(lparen + lparen + columnNameList + rparen + ',' + columnNameList + rparen)
# primary key
primaryKeyDefinition = Group(PRIMARY + KEY + compositeKeyDefinition)('primary_key')

# column type
typeIdentifier = ident.addParseAction(downcaseTokens).setName("cql_type")

# column definition
oneColumnDefinition = (Group(primaryKeyDefinition | (columnName + typeIdentifier + Optional(PRIMARY + KEY))))('column')

# column definition list
columnsDefinition = Forward()
columnsDefinition <<= oneColumnDefinition + ZeroOrMore(',' + columnsDefinition)

# define the grammar

# select
selectStmt = (SELECT + ('*' | columnNameList)("columns") +
              FROM + tableName("table") +
              Optional(Group(WHERE + whereExpression)("where")) +
              Optional(Group(limitExpr)("limit")))

insertStmt = (INSERT + INTO + tableName("table") +
              Group('(' + columnNameList("list") + ')')('columns') +
              VALUES + Group('(' + RvalList("list") + ')')('values'))

updateStmt = (UPDATE + tableName("table") + SET +
              Group(setExpression)('set') +
              Optional(Group(WHERE + whereExpression)("where")) +
              Optional(Group(IF + ifExpression)("if")))

deleteStmt = (DELETE +
              FROM + tableName("table") +
              Optional(Group(WHERE + whereExpression)("where")) +
              Optional(Group(IF + EXISTS)("if")))

createTableStmt = (CREATE + TABLE +
                   Optional(Group(IF + NOT + EXISTS)('if')) +
                   tableName("table") +
                   Group('(' + columnsDefinition("columns") + ')')('columns_def'))

useStatement = (USE + keyspaceName)

sqlStmt = (createTableStmt | selectStmt | insertStmt | updateStmt | deleteStmt | useStatement) + ";"
simpleSQL = sqlStmt

if __name__ == "__main__":
    s = "Select A, B , aaks from Sys.dual where a='3' and b=22 limit 99;"
    p = simpleSQL.parseString(s)
    print(p)
    
    s = "Insert into dual (ds,sd) values (1,'33') ;"
    p = simpleSQL.parseString(s)
    print(p)
    
    s = "update dual SET ds=1, ss='3232' where a='3' and b=22 IF a='3' and b=22;"
    p = simpleSQL.parseString(s)
    print(p)
    
    s = "delete from dual where a='3' and b=22 IF EXISTS;"
    p = simpleSQL.parseString(s)
    print(p)
    
    s = "use akaksakhd;"
    p = simpleSQL.parseString(s)
    print(p)
    
    s = """
        CREATE TABLE sblocks (
            id uuid,
            block_id uuid,
            sub_block_id uuid,
            data blob,
            PRIMARY KEY ( (id, block_id), sub_block_id )
        );
    """
    p = simpleSQL.parseString(s)
    print(p)
    
    print(p['columns_def']['columns']['column']['primary_key'][2])
    
    # simpleSQL.runTests("""\
    #
    #     # FAIL no ; terminator
    #     SELECT * from XYZZY
    #
    #     # ok SELECT
    #     select * from XYZZY;
    #
    #     # dotted table name
    #     select * from SYS.XYZZY;
    #
    #     Select A from Sys.dual;
    #
    #     Select A,B,C from Sys.dual;
    #
    #     # FAIL multiple tables
    #     Select A, B, C from Sys.dual, Table2;
    #
    #     # FAIL - invalid SELECT keyword
    #     Xelect A, B, C from Sys.dual;
    #
    #     # FAIL - invalid FROM keyword
    #     Select A, B, C frox Sys.dual;
    #
    #     # FAIL - incomplete statement
    #     Select
    #
    #     # FAIL - incomplete statement;
    #     Select * from
    #
    #     # FAIL - invalid column
    #     Select &&& frox Sys.dual;
    #
    #     # where clause =
    #     Select A from Sys.dual where a='3';
    #
    #     # multiple conditions where clause
    #     Select A, B , aaks from Sys.dual where a='3' and b=22 limit 99;
    #
    #     # where clause eq
    #     Select A from Sys.dual where a eq '3';
    #
    #     # where clause eq
    #     Insert into Sys.dual (ds,sd) values (1,'33') ;
    #
    # """)
