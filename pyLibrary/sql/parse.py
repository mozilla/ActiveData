
from pyparsing import \
    CaselessLiteral, Word, delimitedList, Optional, Combine, Group, alphas, \
    nums, alphanums, Forward, oneOf, ZeroOrMore, restOfLine, Keyword, sglQuotedString, dblQuotedString, \
    Literal, ParserElement

ParserElement.enablePackrat()

from pyLibrary import convert

SELECT = Keyword("select", caseless=True)
FROM = Keyword("from", caseless=True)
WHERE = Keyword("where", caseless=True)
GROUPBY = Keyword("group by", caseless=True)
AS = Keyword("as", caseless=True)
AND = Keyword("and", caseless=True)
OR = Keyword("or", caseless=True)
NOT = Keyword("not", caseless=True)
IN = Keyword("in", caseless=True)

RESERVED = SELECT | FROM | WHERE | GROUPBY | AS | AND | OR | NOT | IN

# NUMBERS
E = CaselessLiteral("E")
# binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-", exact=1)
realNum = Combine(
    Optional(arithSign) +
    (Word(nums) + "." + Optional(Word(nums)) | ("." + Word(nums))) +
    Optional(E + Optional(arithSign) + Word(nums))
)
intNum = Combine(
    Optional(arithSign) +
    Word(nums) +
    Optional(E + Optional("+") + Word(nums))
)


# EXPRESSIONS
ident = (~RESERVED + (Word(alphas, alphanums + "_$") | dblQuotedString)).setName("identifier")
primitive = realNum("literal") | intNum("literal") | sglQuotedString("literal") | ident

expr = Forward()

MultOp = Group(delimitedList(expr, "*"))("mul")
DivOp = Group(delimitedList(expr, "/"))("div")
term = primitive | MultOp | DivOp

AddOp = Group(delimitedList(term, "+"))("add")
SubOp = Group(delimitedList(term, "-"))("sub")

EqOp = Group(delimitedList(expr, "="))("eq")
NeqOp = Group(delimitedList(expr, "!="))("neq")
GtOp = Group(delimitedList(expr, ">"))("gt")
GteOp = Group(delimitedList(expr, ">="))("gte")
LtOp = Group(delimitedList(expr, "<"))("lt")
LteOp = Group(delimitedList(expr, "<="))("lte")

BooleanOp = EqOp | NeqOp | GtOp | GteOp | LtOp | LteOp | AddOp | SubOp | expr
NotOp = Group(NOT + BooleanOp)("not") | BooleanOp
AndOp = Group(delimitedList(NotOp, AND))("and") | NotOp
OrOp = Group(delimitedList(AndOp, OR))("or") | AndOp

selectStmt = Forward()
expr << Group(
    ("(" + expr + ")") |
    (primitive("var") + IN + "(" + delimitedList(expr)("list") + ")")("in") |
    (primitive("var") + IN + "(" + selectStmt("set") + ")")("in") |
    (Word(alphas)("op") + "(" + Group(delimitedList(expr))("params") + ")") |
    primitive
)





# whereBoolean = Forward()
# whereExpression = Group(
#     (NOT + whereBoolean) |
#     (Word(alphas)("op") + "(" + Group(delimitedList(whereBoolean))("params") + ")") |
#     ("(" + whereBoolean + ")") |
#     (primitive + IN + "(" + delimitedList(primitive) + ")") |
#     (primitive + IN + "(" + selectStmt + ")") |
#     primitive + ZeroOrMore(binop + whereBoolean)
# )
# whereBoolean << whereExpression + ZeroOrMore((AND | OR) + whereBoolean)

# SQL STATEMENT
columnName = (delimitedList(ident, ".", combine=True)).setName("column name")
column = Group(
    (expr("value") + AS + columnName("name")) |
    (expr("value") + columnName("name")) |
    expr("value") |
    Literal('*')("value")
)
tableName = (delimitedList(ident, ".", combine=True)).setName("table name")

# define SQL tokens
selectStmt << (
    SELECT + delimitedList(column)("select") +
    FROM + delimitedList(tableName)("tables") +
    Optional(WHERE + expr)("where") +
    Optional(GROUPBY + delimitedList(column))("groupby")
)

simpleSQL = selectStmt

# IGNORE SOME COMMENTS
oracleSqlComment = Literal("--") + restOfLine
mySqlComment = Literal("#") + restOfLine
simpleSQL.ignore(oracleSqlComment | mySqlComment)

if __name__ == "__main__":
    simpleSQL.runTests(
        """

        # multiple tables
        SELECT * from XYZZY, ABC

        # dotted table name
        select * from SYS.XYZZY

        Select A from Sys.dual

        Select A,B,C from Sys.dual

        Select A, B, C from Sys.dual, Table2

        # FAIL - invalid SELECT keyword
        Xelect A, B, C from Sys.dual

        # FAIL - invalid FROM keyword
        Select A, B, C frox Sys.dual

        # FAIL - incomplete statement
        Select

        # FAIL - incomplete statement
        Select * from

        # FAIL - invalid column
        Select &&& frox Sys.dual

        # where clause
        Select A from Sys.dual where a in ('RED','GREEN','BLUE')

        # compound where clause
        Select A from Sys.dual where a in ('RED','GREEN','BLUE') and b in (10,20,30)

        # where clause with comparison operator
        Select A,b from table1,table2 where table1.id = table2.id

        # group by
        select a, count(1) as b from mytable group by a

        """
    )
