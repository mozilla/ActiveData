two_dim_test_data = [
    {"a": "x", "b": "m", "v": 2},
    {"a": "x", "b": "n", "v": 3},
    {"a": "x", "b": None, "v": 5},
    {"a": "y", "b": "m", "v": 7},
    {"a": "y", "b": "n", "v": 11},
    {"a": "y", "b": None, "v": 13},
    {"a": None, "b": "m", "v": 17},
    {"a": None, "b": "n", "v": 19},
    {"a": "x", "b": "m", "v": 27},
    {"a": "y", "b": "n", "v": 39}
]

metadata = {
    "properties": {
        "a": {
            "type": "string",
            "domain": {
                "type": "set",
                "partitions": ["x", "y", "z"]
            }
        },
        "b": {
            "type": "string",
            "domain": {
                "type": "set",
                "partitions": ["m", "n"]
            }
        }
    }
}

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
        "name": "count rows, 2d",
        "metatdata": {},
        "data": two_dim_test_data,
        "query": {
            "from": "testdata",
            "select": {"aggregate": "count"},
            "edges": ["a", "b"]
        },
        "expecting_list": [
            {"a": "x", "b": "m", "count": 2},
            {"a": "x", "b": "n", "count": 1},
            {"a": "x", "b": None, "count": 1},
            {"a": "y", "b": "m", "count": 1},
            {"a": "y", "b": "n", "count": 2},
            {"a": "y", "b": None, "count": 1},
            {"a": "z", "b": "m", "count": 0},
            {"a": "z", "b": "n", "count": 0},
            {"a": "z", "b": None, "count": 0},
            {"a": None, "b": "m", "count": 1},
            {"a": None, "b": "n", "count": 1},
            {"a": None, "b": None, "count": 0}
        ],
        "expecting_table": {
            "header": ["a", "b", "count"],
            "data": [
                ["x", "m", 2],
                ["x", "n", 1],
                ["x", None, 1],
                ["y", "m", 1],
                ["y", "n", 2],
                ["y", None, 1],
                ["z", "m", 0],
                ["z", "n", 0],
                ["z", None, 0],
                [None, "m", 1],
                [None, "n", 1],
                [None, None, 0]
            ]
        },
        "expecting_cube": {
            "edges": [
                {
                    "name": "a",
                    "type": "string",
                    "allowNulls": True,
                    "domain": {
                        "type": "set",
                        "partitions": ["x", "y", "z"]
                    }
                },
                {
                    "name": "b",
                    "type": "string",
                    "allowNulls": True,
                    "domain": {
                        "type": "set",
                        "partitions": ["m", "n"]
                    }
                }
            ],
            "data": {
                "count": [
                    [2, 1, 1],
                    [1, 2, 1],
                    [0, 0, 0],
                    [1, 1, 0]
                ]
            }
        }
    },

    {
        "name": "sum rows",
        "metatdata": {},
        "data": two_dim_test_data,
        "query": {
            "from": "testdata",
            "select": {"value": "v", "aggregate": "sum"},
            "edges": ["a", "b"]
        },
        "expecting_list": [
            {"a": "x", "b": "m", "v": 29},
            {"a": "x", "b": "n", "v": 3},
            {"a": "x", "b": None, "v": 5},
            {"a": "y", "b": "m", "v": 7},
            {"a": "y", "b": "n", "v": 50},
            {"a": "y", "b": None, "v": 13},
            {"a": "z", "b": "m", "v": None},
            {"a": "z", "b": "n", "v": None},
            {"a": "z", "b": None, "v": None},
            {"a": None, "b": "m", "v": 17},
            {"a": None, "b": "n", "v": 19},
            {"a": None, "b": None, "v": None}
        ],
        "expecting_table": {
            "header": ["a", "b", "v"],
            "data": [
                ["x", "m", 29],
                ["x", "n", 3],
                ["x", None, 5],
                ["y", "m", 7],
                ["y", "n", 50],
                ["y", None, 13],
                ["z", "m", None],
                ["z", "n", None],
                ["z", None, None],
                [None, "m", 17],
                [None, "n", 19],
                [None, None, None]
            ]
        },
        "expecting_cube": {
            "edges": [
                {
                    "name": "a",
                    "type": "string",
                    "allowNulls": True,
                    "domain": {
                        "type": "set",
                        "partitions": ["x", "y", "z"]
                    }
                },
                {
                    "name": "b",
                    "type": "string",
                    "allowNulls": True,
                    "domain": {
                        "type": "set",
                        "partitions": ["m", "n"]
                    }
                }
            ],
            "data": {
                "v": [
                    [29, 3, 5],
                    [7, 50, 13],
                    [None, None, None],
                    [17, 19, None]
                ]
            }
        }
    },


]
