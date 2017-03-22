

SQL Shortcomings
================

Here is a unordered list of the anti-patterns I see when dealing with SQL.
It is a personal note to myself, but I hope to expand it to explain the
benefits of JSON query expressions.

This document serves to provide motivation for a query language beyond SQL. JSON query expressions are a query language optimized specifically for hierarchical databases, nested JSON, and data warehouses.  

Some common requests that are hard to don in SQL
http://www2.sqlite.org/cvstrac/wiki?p=UnsupportedSqlAnalyticalFunctions


* **Calculate a running total** - Show the cumulative salary within a department row by row, with each row including a summation of the prior rows' salary.
* **Find percentages within a group** - Show the percentage of the total salary paid to an individual in a certain department. Take their salary and divide it by the sum of the salary in the department.
* **Top-N queries** - Find the top N highest-paid people or the top N sales by region.
* **Compute a moving average** - Average the current row's value and the previous N rows values together.
* **Perform ranking queries** - Show the relative rank of an individual's salary within their department. 



JSON Query Expressions vs SQL
-----------------------------

A deliberate feature of a JSON expressions is it's JSON. It can be easily declared in Python and Javascript, and easily manipulated by code.  

Many of the SQL's shortcomings, which I touch on below, are overcome by string concatenation on client-side code. Good ORM libraries will formalize this string manipulation with a series of function calls, which are used to create a abstract syntax tree, which is serialized to SQL. SQLAlchemy is a particularly good ORM because it leverages Python's magic methods to make elegant imperative Python expressions generate those data structures behind the scenes. But, in every case, you are running code that generates a data structure, which is then used to generate SQL.      

	ORM Expressions -> AST -> SQL -> network -> SQL -> AST -> Query

JSON expressions are slightly better in this regard; It is its own AST, and does not require serialization to a complex intermediate language. Furthermore, an ORM library would be trivial to write, so trivial that it would provide negligible benefit over simply stating the JSON structure directly.

	JSON Expressions -> JSON -> network -> JSON -> JSON Expressions -> Query 

### Splitting credit and debit

Sometimes we would like to clarify positive and negative numbers in separate
columns, like in accounting:

    SELECT
        account_number,
        CASE WHEN sum(amount)>0 THEN amount ELSE NULL END credit,
        CASE WHEN sum(amount)<0 THEN amount ELSE NULL END debit
    FROM
        transactions

JSON Expressions can (re)use domain definitions to abstract-away the query complexities.  For example, consider the `money` domain:

    money = {
        "name":"money",
        "type":"set",
        "partitions":[
            {"name":"credit", "where":{"gte":{"amount": 0}}},
            {"name":"debit", "where":{"lt":{"amount": 0}}},
        ]
    }

Domains are useful abstraction that give names to complex business rules.  Our `money` example is not a complicated domain, so the JSON Query Expression  

    {
    "from": "transactions",
    "select": [
		"account_number",
		"money*"
    }


Partitioning records along more dimensions gets more painful with SQL:

    SELECT
        account_number,
        CASE WHEN txn_type='SEND' THEN principal_amount ELSE 0 end SendAmount,
        CASE WHEN txn_type='SEND' THEN charges else 0 end SendFees,
        CASE WHEN txn_type='RECEIVE' THEN principal_amount ELSE 0 end RefundAmount,
        CASE WHEN txn_type='RECEIVE' THEN charges else 0 end RefundFees
    FROM
        transfers

We put the business logic into a domain definition

    charge_breakdown = {
        "name":"Charge Breakdown",
        "type":"set",
        "partitions":[
            {"name":"SendAmount",   "value":"principal_amount", "where":{"eq":{"txn_type": "SEND"}},
            {"name":"SendFees",     "value":"charges",          "where":{"eq":{"txn_type": "SEND"}},
            {"name":"RefundAmount", "value":"principal_amount", "where":{"eq":{"txn_type": "RECEIVE"}},
            {"name":"RefyundFees",  "value":"charges",          "where":{"eq":{"txn_type": "RECEIVE"}},
        ]
    }

And 

    {
    "from": "transactions",
    "select": [
        "account_number",
		"charge_breakdown*"
    ]
    }


Filter by Rank
--------------


Here is a common SQL pattern:

```sql
    SELECT
      *
    FROM
      alerts.alerts a
    LEFT JOIN
      (
        SELECT
          branch,
          MAX(last_updated) max_date
        FROM
          alerts.alerts
        WHERE
          revision like '%c1f6%'
        GROUP BY
          branch
      ) m ON m.branch = a.branch AND m.max_date=a.last_updated
    WHERE
      revision like '%c1f6%'
    ;
```

The high level objective of this code is to pick the latest record from each
branch. But, this code is wrong; possibly returning more than one record per
category. The example is also very simple, and ranking algorithms can get
complicated. Given we got this one wrong, we have little chance of writing
complicated ranking algorithms correctly.

Window functions almost do what we want: They can categorize using edges, and
rank using sort, but they do not change the number of rows returned.
Generally, we what a clause that can pick the "best" record according to some
ranking algorithm, by category.


I suggest a new `having` clause, as per SQL, but with additional parameters to
specify grouping and more-detailed ordering:

```javacript
    {
        "from":"alerts",
        "where":{"regexp":{"revision":".*c1f6.*"}},
        "having":{"edges":["branch"], "sort":["last_updated"], "rank":"last"}
    }
```

If you really wanted dups, we can use "maximum"

```javacript
    {
        "from":"alerts",
        "where":{"regexp":{"revision":".*c1f6.*"}},
        "having":{"edges":["branch"], "sort":["last_updated"], "rank":"maximum"}
    }
```

Here is an example of how simple we can go:

<html><table><tr><td>
<pre>
    SELECT
      *
    FROM
      alerts a
    LEFT JOIN
      (
        SELECT
          MAX(last_updated) max_date
        FROM
          alerts
        WHERE
          revision like '%c1f6%'
      ) m ON m.max_date=a.last_updated
    WHERE
      revision like '%c1f6%'
    ;
</pre></td><td><pre>
{
    "from":"alerts",
    "where":{"regexp":{"revision":".*c1f6.*"}},
    "having":{"sort":["last_updated"], "rank":"maximum"}
}
</pre>
</td></tr></table>


Reporting multiple dimensions as columns

    sum(CASE WHEN code='thisDay' THEN num_opened else 0 end) thisDay_opened,
    sum(CASE WHEN code='thisDay' THEN num_closed else 0 end) thisDay_closed,
    sum(CASE WHEN code='this7Day' THEN num_opened else 0 end) this7Day_opened,
    sum(CASE WHEN code='this7Day' THEN num_closed else 0 end) this7Day_closed,
    sum(CASE WHEN code='thisMonth' THEN num_opened else 0 end) thisMonth_opened,
    sum(CASE WHEN code='thisMonth' THEN num_closed else 0 end) thisMonth_closed,
    sum(CASE WHEN code='lastMonth' THEN num_opened else 0 end) lastMonth_opened,
    sum(CASE WHEN code='lastMonth' THEN num_closed else 0 end) lastMonth_closed,

Normalizing to 1000's or millions

    sum(CASE WHEN code='thisDay' THEN 1000000*(num_opened)/population else 0 end) thisDay_opened_pct,
    sum(CASE WHEN code='thisDay' THEN 1000000*(num_closed)/population else 0 end) thisDay_closed_pct,
    sum(CASE WHEN code='this7Day' THEN 1000000*(num_opened)/population else 0 end) this7Day_opened_pct,
    sum(CASE WHEN code='this7Day' THEN 1000000*(num_closed)/population else 0 end) this7Day_closed_pct,
    sum(CASE WHEN code='thisMonth' THEN 1000000*(num_opened)/population else 0 end) thisMonth_opened_pct,
    sum(CASE WHEN code='thisMonth' THEN 1000000*(num_closed)/population else 0 end) thisMonth_closed_pct,
    sum(CASE WHEN code='lastMonth' THEN 1000000*(num_opened)/population else 0 end) lastMonth_opened_pct,
    sum(CASE WHEN code='lastMonth' THEN 1000000*(num_closed)/population else 0 end) lastMonth_closed_pct,

Left join of dimension and ALL partitions

    FROM
        (
        SELECT
            p.zone,
            max(ordering) ordering,
            sum(population) population
        FROM
            provinces p
        GROUP BY
            p.zone
        ) p
    LEFT JOIN
        BLAH BLAH


Again, the partitions

    LEFT JOIN
        temp_time_ranges r
    ON
        time_convert(r.mindate, 'EDT', 'GMT') <= s.date AND
        s.date < time_convert(r.maxdate, 'EDT', 'GMT')


Showing both volume and count

    sum(CASE WHEN r.code='this90Day' THEN s.quantity ELSE 0 END)/90*30 quantity_90,
    sum(CASE WHEN r.code='this90Day' THEN s.volume ELSE 0 END)/90*30 volume_90,

Show by hour of day, spit by column

    hour(time_convert(t.transaction_date, 'GMT', 'EDT')) hour_of_day,
    sum(CASE WHEN t.type NOT IN('Fees', 'Load-Bill Payment', 'Corporate Load', 'Load-Bank Transfer') THEN 1 ELSE 0 END)/91*30 numOther,
    sum(CASE WHEN t.type='Fees' THEN 1 ELSE 0 END)/91*30 numFees,
    sum(CASE WHEN t.type='Load-Bill Payment' THEN 1 ELSE 0 END)/91*30 numBillPayment,
    sum(CASE WHEN t.type='Load-Bank Transfer' THEN 1 ELSE 0 END)/91*30 numBankTransfer,
    sum(CASE WHEN t.type='Corporate Load' THEN 1 ELSE 0 END)/91*30 numCorporate


Report by timezone, using num open, num closed, and net

    hour(time_convert(t.transaction_date, 'GMT', 'EDT')) hour_of_day,
    sum(CASE WHEN t.type NOT IN('Fees', 'Load-Bill Payment', 'Corporate Load', 'Load-Bank Transfer') THEN 1 ELSE 0 END)/91*30 numOther,
    sum(CASE WHEN t.type='Fees' THEN 1 ELSE 0 END)/91*30 numFees,
    sum(CASE WHEN t.type='Load-Bill Payment' THEN 1 ELSE 0 END)/91*30 numBillPayment,
    sum(CASE WHEN t.type='Load-Bank Transfer' THEN 1 ELSE 0 END)/91*30 numBankTransfer,
    sum(CASE WHEN t.type='Corporate Load' THEN 1 ELSE 0 END)/91*30 numCorporate


The benefit of partitions is that they are guaranteed to not overlap. In this
case, the `Other` part is left with all remaining transaction types. There is
no double counting, and no missed values.

```javascript
    payType = {
        "name":"payType",
        "type":"set",
        "partitions":[
            {"name":"Fees", "where":{"term":{"type":"Fees"}}},
            {"name":"BillPayment", "where":{"term":{"type":"Load-Bill Payment"}}},
            {"name":"BankTransfer", "where":{"term":{"type":"Load-Bank Transfer"}}},
            {"name":"Corporate", "where":{"term":{"type":"Corporate Load"}}},
            {"name":"Other"}
        ]
    }

    query = {
        "from":transactions
        "select":{"name":"num", "aggregate":"count"}
        "edges":[
            {"domain":payType}
        ]
    }
```

If data can be split according to independent criterion, then you avoid the
inevitable power-set that results.

    SELECT
        count(1) `count`
        CASE
        WHEN card_number IS NOT NULL AND b.bank_number IS NOT NULL AND b.autoload=1 THEN 'has Both w Auto'
        WHEN card_number IS NOT NULL AND b.bank_number IS NOT NULL AND b.autoload=0 THEN 'has Both wo Auto'
        WHEN card_number IS NOT NULL AND b.bank_number IS NOT NULL THEN 'has Both'
        WHEN card_number IS NOT NULL THEN 'has Card'
        WHEN b.autoload=1 THEN 'has Bank w Auto'
        WHEN b.autoload=0 THEN 'has Bank wo Auto'
        WHEN b.account_number IS NOT NULL THEN 'has Bank'
        ELSE 'No bank or card'
        END category,
    FROM
        accounts




    cardStatusDomain = {
        "name":"category",
        "type":"set",
        "partitions":[
            {"name":"has Both", "where":{"and":[
                {"exists":"card_number"},
                {"exists":"account_number"}
            ]},
            {"name":"has Card", "where":{"exists":"card_number"}},
            {"name":"has Bank", "where":{"exists":"account_number"}},
            {"name":"No bank or card"}
        ]
    }

    autoDomain = {
        "name":"category",
        "type":"set",
        "partitions":[
            {"name":"w Auto", "where":{"eq":{"autoload": 1}}},
            {"name":"wo Auto", "where":{"eq":{"autoload": 0}}},
        ]
    }

    {
    "from":accounts.
    "select":{"name":"count", "aggregate":"count"}
    "edges":[
        {"name":"status", "domain":cardStatusDomain},
        {"name":"auto", "domain":autoDomain}
    ]
    }


The using partition order to define the mutually exclusive sets reduces total
number of rules written, but the order of presentation may be different

    ORDER BY
        CASE
        WHEN w.is_Active=1 THEN 2
        WHEN w.is_New=1 THEN 1
        WHEN w.is_used=1 THEN 3
        WHEN (w.is_open=1 or o.accountstatus='OPEN') AND o.dateopened<=r.mindate THEN 5
        WHEN w.is_open=1 or o.accountstatus='OPEN' THEN 4
        ELSE 6
        END ordering,

And then same logic to show name

    SELECT
        CASE
        WHEN w.is_Active=1 THEN 'is active'
        WHEN w.is_New=1 THEN 'is new'
        WHEN w.is_used=1 THEN 'is used'
        WHEN (w.is_open=1 or o.accountstatus='OPEN') AND o.dateopened<=r.mindate THEN 'is_neglected'
        WHEN w.is_open=1 or o.accountstatus='OPEN' THEN 'is_open'
        ELSE 'is Closed'
        END status,

    status


    accountStatusDomain = {
        "name":"status",
        "type":"set",
        "partitions":[
            {"name":"Active", "where":{"term":{"is_active": 1}}},
            {"name":"New", "where":{"term":{"is_new": 1}}},
            {"name":"Used", "where":{"term":{"is_used": 1}}},
            {
                "name":"Neglected",
                "where":{"and":[
                    {"or":[
                        {"term":{"is_open": 1}},
                        {"term":{"accountstatus":"OPEN"}}
                    ]},
                    {"range":{"dateopened":{"lte":"{{mindate}}
            },
            {
                "name":"Open",
                "where": {"or":[
                    {"term":{"is_open": 1}},
                    {"term":{"accountstatus":"OPEN"}}
                ]}
            },
            {"name":"Closed"}
        ]
    }

    {
        "from":accounts,
        "select":[
            {"value":"balance", "aggregate":"average"},
            {"name":"count", "aggregate":"count"}
        ],
        "edges":[{"domain":accountStatusDomain}]
    }



### Summarize Everything

You would think a database constraint would avoid certain impossibilities, but
you would be wrong: There are legal reasons the foreign key can be
missing:

    SELECT
        a.name fullname,
        count(t.id) num_transactions
    FROM
        transactions t
    LEFT JOIN
        accounts w ON w.account_number=t.account_number
    GROUP BY
        t.account_number

Using JSON Expressions, you always aggregate everything in the from clause:

    {
    "from":transactions
    "select":{"name":"num_transactions", "aggregate":"count"},
    "edges":[
        {
            "name":"fullname",
            "label":"name",
            "value":"account_number",
            "domain":{"type":"set", "key":"account_number", "partitions":accounts}
        }
    ]
    }

Table of "standard", but not logical, partitions

    LEFT JOIN
        standard_load_sizes s ON s.minamount<=ABS(t.amount) AND abs(t.amount)<s.maxamount

Converting a 13week sample into a monthly values

    category,
    count(account_number)/91*30 num_loads,
    avg(amount) average_load,
    sum(amount)/91*30 total_volume,
    count(distinct account_number) num_accounts,
    sum(amount)/91*30/count(distinct account_number) volume_per_account,
    count(account_number)/91*30/count(distinct account_number) loads_per_account


Showing partitions as columns

        sum(CASE WHEN r.code='lastWeek13' THEN t.amount ELSE null END) lastWeek13,
        sum(CASE WHEN r.code='lastWeek12' THEN t.amount ELSE null END) lastWeek12,
        sum(CASE WHEN r.code='lastWeek11' THEN t.amount ELSE null END) lastWeek11,
        sum(CASE WHEN r.code='lastWeek10' THEN t.amount ELSE null END) lastWeek10,
        sum(CASE WHEN r.code='lastWeek9' THEN t.amount ELSE null END) lastWeek9,
        sum(CASE WHEN r.code='lastWeek8' THEN t.amount ELSE null END) lastWeek8,
        sum(CASE WHEN r.code='lastWeek7' THEN t.amount ELSE null END) lastWeek7,
        sum(CASE WHEN r.code='lastWeek6' THEN t.amount ELSE null END) lastWeek6,
        sum(CASE WHEN r.code='lastWeek5' THEN t.amount ELSE null END) lastWeek5,
        sum(CASE WHEN r.code='lastWeek4' THEN t.amount ELSE null END) lastWeek4,
        sum(CASE WHEN r.code='lastWeek3' THEN t.amount ELSE null END) lastWeek3,
        sum(CASE WHEN r.code='lastWeek2' THEN t.amount ELSE null END) lastWeek2,
        sum(CASE WHEN r.code='lastWeek' THEN t.amount ELSE null END) lastWeek,
        sum(CASE WHEN r.code='thisWeek' THEN t.amount ELSE null END) thisWeek

Default values when no data is present

    UNION ALL
        SELECT
            '`empty',
            '`empty',
            '`empty',
            null,null,null,null,null,null,null,null,null,null,null,null,null,null
        FROM
            util_digits d
    ) b

using distinct to determine what the partitions are

    FROM
        (
        SELECT DISTINCT
            resource_index,
            resource
        FROM
            analysis.log_performance
        WHERE
            resource like '/backoffice/%'
        ) a
    LEFT JOIN
        analysis.log_performance_backoffice b on b.resource=a.resource

### Quazi-LogScale Tables

SQL demands I build a table that represents the irregular, but intuitive, data partitions. Tables seem heavy-weight compared to a domain definition; if only because the details are realized as records, and those records must be constructed explicitly.

    CREATE PROCEDURE temp_fill_log_performance_ranges ()
    BEGIN
        DECLARE v INTEGER;
        DECLARE min_ INTEGER;
        DECLARE max_ INTEGER;

        ## LOG SCALE
        SET v=0;
        WHILE v<30 DO
            SET min_=round(pow(10, v/6), 0);
            if (v=0) THEN set min_=0; END IF;
            SET max_=round(pow(10, (v+1)/6), 0);
            INSERT INTO log_performance_ranges VALUES (
                'log',
                concat(min_, 'ms - ', (max_-1), 'ms'),
                min_,
                max_
            );
            SET v=v+1;
        END WHILE;

        ## MILLISECOND SCALE
        SET v=0;
        WHILE v<30 DO
            SET min_=v*100;
            SET max_=(v+1)*100;
            INSERT INTO log_performance_ranges VALUES (
                'ms',
                concat(min_, 'ms - ', (max_-1), 'ms'),
                min_,
                max_
            );
            SET v=v+1;
        END WHILE;

        ## SECOND SCALE
        SET v=0;
        WHILE v<30 DO
            SET min_=v*1000;
            SET max_=(v+1)*1000;
            INSERT INTO log_performance_ranges VALUES (
                'sec',
                concat(v, 'sec - ', (v+1), 'sec'),
                min_,
                max_
            );
            SET v=v+1;
        END WHILE;

    END;;

Reporting the top N (based on larger sample), even though daily samples do not have same order

    LEFT JOIN
        (# TOP 5 COUNTRIES
        SELECT
            coalesce(t.country, 'tst') country,
            sum(t.amount) amount
        FROM
            temp_Executive_WU_txns t
        WHERE
            t.dateRange='this30Day'
            AND    t.type='Send'
        GROUP BY
            coalesce(t.country, 'tst')
        ORDER BY
            sum(t.amount) DESC
        LIMIT
            5

Dimension rollup

    INSERT INTO transaction_types (code, description, type) VALUES ('AC','Account Closure','Account Closure');
    INSERT INTO transaction_types (code, description, type) VALUES ('ACTF','Account Fee','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('ACTFF','Account Fee Fee','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('ACTFR','Account Fee Refund','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATF','Activation Fee','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATFF','Activation Fee Fee','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATFR','Activation Fee Refund','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATITI','Issuer to Issuer Account Transfer','Cash');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATITIF','Issuer to Issuer Account Transfer Fee','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATSTS','Satellite to Satellite Account Transfer','Cash');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATSTSF','Satellite to Satellite Account Transfer Fee','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATSTW','Satellite to account Account Transfer','Cash');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATSTWF','Satellite to account Account Transfer Fee','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATWTS','account to Satellite Account Transfer','Cash');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATWTSF','account to Satellite Account Transfer Fee','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATWTW','account to account Account Transfer','Cash');
    INSERT INTO transaction_types (code, description, type) VALUES ('ATWTWF','account to account Account Transfer Fee','Fees');
    INSERT INTO transaction_types (code, description, type) VALUES ('BF','Bulk Beam Fee','Fees');

Ordering, roll-up and style

    INSERT INTO categories (ordering, type, color, group) VALUES (1, 'Load-Credit Card','blue', 'Load');
    INSERT INTO categories VALUES (2, 'Retail Sales','blue', 'Spend');
    INSERT INTO categories VALUES (3, 'Savings','red', 'Spend');
    INSERT INTO categories VALUES (4, 'Previous Savings','red', 'Load');
    INSERT INTO categories VALUES (5, 'P2P Send','p2p', 'Spend');
    INSERT INTO categories VALUES (7, 'P2P Receive','p2p', 'Load');
    INSERT INTO categories VALUES (9, 'Load-Bank Transfer','green', 'Load');
    INSERT INTO categories VALUES (10, 'Load-Bill Payment','light green', 'Load');
    INSERT INTO categories VALUES (11, 'Interac','very light green', 'Load');
    INSERT INTO categories VALUES (12, 'ATM','green', 'Spend');
    INSERT INTO categories VALUES (13, 'Cashout','light green', 'Spend');
    INSERT INTO categories VALUES (14, 'WU Send','yellow', 'Spend');
    INSERT INTO categories VALUES (15, 'WU Pickup','yellow', 'Load');
    INSERT INTO categories VALUES (16, 'Corporate Load','purple', 'Load');
    INSERT INTO categories VALUES (17, 'Adjustment Load','cyan', 'Load');
    INSERT INTO categories VALUES (18, 'Adjustment Unload','cyan', 'Spend');
    INSERT INTO categories VALUES (19, 'Fees','light cyan', 'Spend');


In SQL it is important to ```LEFT JOIN``` the categories in the event there are
zero transactions in that category. We also require an explicit ```ORDER BY```
to maintain consistent presentation.

    SELECT
        c.type,
        sum(amount) total,
        c.color
    FROM
        categories c
    LEFT JOIN
        transactions t on t.type=c.type
    GROUP BY
        c.type,
        c.color
    ORDER BY
        c.ordering


    {
    "from":"transactions",
    "select":{"name":"total", "value":"amount", "aggregate":"sum"},
    "edges"[
        {"value":"type", "domain":"categories"}
    ]
    }






Parsing data into columns

    SELECT
        data_source, #JUST THE FILENAME WITHOUT THE PATH
        now(), #datadate
        util_newid(), #id
        string_get_word(log_import_app_gateway_parse_signup(data), '|', 0), #logtype
        string_get_word(log_import_app_gateway_parse_signup(data), '|', 1), #userid
        string_get_word(log_import_app_gateway_parse_signup(data), '|', 2), #action
        string_get_word(log_import_app_gateway_parse_signup(data), '|', 3), #useragent
        string_get_word(log_import_app_gateway_parse_signup(data), '|', 4), #devicetype
        string_get_word(log_import_app_gateway_parse_signup(data), '|', 5), #firstname
        string_get_word(log_import_app_gateway_parse_signup(data), '|', 6), #lastname
        string_get_word(log_import_app_gateway_parse_signup(data), '|', 7), #mobile
        string_get_word(log_import_app_gateway_parse_signup(data), '|', 8), #email
        str_to_date(substring(data, 1, 23), '%Y-%m-%d %H:%i:%s,%f') #timestamp
    FROM
        temp_import_log_application l

Rule-based partitions

        CASE
        WHEN t.amount<0 THEN NULL #'neg receive'
        WHEN bigrecipient.account_number IS NULL AND t.transactiontype='WU_PICKUP' THEN 'WU Pickup'
        WHEN bigsender.account_number IS NOT NULL THEN 'Corporate Load'
        WHEN t.transactiontype='XF' THEN 'P2P Receive'
        WHEN bigrecipient.account_number IS NOT NULL THEN NULL #'to promo'
        ELSE 'Unknown'
        END category

One record results in two or more output records

    CASE
        WHEN d.digit=0 AND c.amount IS NULL THEN t.amount
        WHEN d.digit=0 THEN -c.amount
        WHEN d.digit=1 AND c.amount<>-t.amount THEN t.amount+c.amount
        ELSE t.amount
        END)/100, 2) volume,
        CASE
        WHEN d.digit=0 AND c.amount IS NULL and t.amount<0 THEN 'Adjustment Unload'
        WHEN d.digit=0 AND c.amount IS NULL THEN 'Adjustment Load'
        WHEN d.digit=0 THEN 'Cashout'
        WHEN d.digit=1 AND c.amount<>-t.amount THEN 'Fees'
        ELSE NULL
        END category
    FROM
        transactions t
    LEFT JOIN
        cashoutrequests c ON c.referencenumber=t.journalnumber AND t.account_number=c.account_number
    LEFT JOIN
        temp_exeutive_account_is_ignored bigsender on bigsender.account_number=t.account_number
    LEFT JOIN
        util_digits d on d.digit<2



Easy importing

Get all in directory

Clustered indexes for speed

Filling in missing parts of domain

    DECLARE @datadate DATETIME
    SET @datadate=dbo.to_date('20070225', 'YYYYMMDD')
    WHILE @datadate<getdate() BEGIN
        INSERT INTO ctx_missing VALUES ('flat_'+CONVERT(VARCHAR, @datadate, 12)+'.txt', @datadate)
        SET @datadate=dateAdd(month, 1, @datadate)
    END

Filling in missing parts of domain


    exec _drop 'ctx_existing'
    SELECT
        datasource
    INTO
        ctx_existing
    FROM
        bill
    WHERE
        datasource NOT LIKE '%autogen%'
    GROUP BY
        datasource

    DELETE FROM ctx_missing WHERE datasource IN (
        SELECT datasource FROM ctx_existing
    )
    go


Setting default values, replacing invalid values:

    update bill set
        quantity=1
    where
        quantity is null or
        quantity=0


Standard date format

    to_Date('01-JAN-2012', 'DD-MON-YYYY')
    str_to_date("2012-01-01", "%Y-%m-%d")
    Date.newInstance("2012-01-01", "yyyy-MM-dd");


Copying whole lists of columns
