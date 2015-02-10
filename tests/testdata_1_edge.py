simple_test_data = [
    {"a": "c", "v": 13},
    {"a": "b", "v": 2},
    {"v": 3},
    {"a": "b", "v": 5},
    {"a": "c", "v": 7},
    {"a": "c", "v": 11}
]

tests = [


    {
        "name": "EXAMPLE TEMPLATE",
        "metatdata": {},  # OPTIONAL DATA SHAPE REQUIRED FOR NESTED DOCUMENT QUERIES
        "data": [],  # THE DOCUMENTS NEEDED FOR THIS TEST
        "query": {  # THE Qb QUERY
                    "from": "testdata",  # "testdata" WILL BE REPLACED WITH DATASTORE FILLED WITH data
                    "edges": []  # THIS FILE IS EXPECTING EDGES (OR GROUP BY)
        },
        "expecting": []  # THE EXPECTED RESULT (INCLUDING METADATA)
    },


    {
        "name": "count rows, 1d",
        "metatdata": {},
        "data": simple_test_data,
        "query": {
            "from": "testdata",
            "select": {"aggregate": "count"},
            "edges": ["a"]
        },
        "expecting_list": [
            {"a": "b", "count": 2},
            {"a": "c", "count": 3},
            {"a": None, "count": 1}
        ],
        "expecting_table": {
            "header": ["a", "count"],
            "data": [
                ["b", 2],
                ["c", 3],
                [None, 1]
            ]
        },
        "expecting_cube": {
            "edges": [
                {
                    "name": "a",
                    "type": "set",
                    "allowNulls": True,
                    "domain": {
                        "partitions": ["b", "c"]
                    }
                }
            ],
            "data": {
                "count": [2, 3, 1]
            }
        }
    },

    {
        "name": "count column",
        "metatdata": {},
        "data": simple_test_data,
        "query": {
            "from": "testdata",
            "select": {"name":"count_a", "value": "a", "aggregate": "count"},
            "edges": ["a"]
        },
        "expecting_list": [
            {"a": "b", "count_a": 2},
            {"a": "c", "count_a": 3},
            {"a": None, "count_a": 0}
        ],
        "expecting_table": {
            "header": ["a", "count_a"],
            "data": [
                ["b", 2],
                ["c", 3],
                ["a", 0]
            ]
        },
        "expecting_cube": {
            "edges": [
                {
                    "name": "a",
                    "type": "set",
                    "allowNulls": True,
                    "domain": {
                        "partitions": ["b", "c"]
                    }
                }
            ],
            "data": {
                "count_a": [2, 3, 0]
            }
        }
    },

    {
        "name": "sum column",
        "metatdata": {},
        "data": simple_test_data,
        "query": {
            "from": "testdata",
            "select": {"value": "v", "aggregate": "sum"},
            "edges": ["a"]
        },
        "expecting_list": [
            {"a": "b", "v": 7},
            {"a": "c", "v": 31},
            {"a": None, "v": 3}
        ],
        "expecting_table": {
            "header": ["a", "v"],
            "data": [
                ["b", 7],
                ["c", 31],
                [None, 3]
            ]
        },
        "expecting_cube": {
            "edges": [
                {
                    "name": "a",
                    "type": "set",
                    "allowNulls": True,
                    "domain": {
                        "partitions": ["b", "c"]
                    }
                }
            ],
            "data": {
                "v": [7, 31, 3]
            }
        }
    },





]
