tests = [


    {
        "name": "EXAMPLE TEMPLATE",
        "metatdata": {},         # OPTIONAL DATA SHAPE REQUIRED FOR NESTED DOCUMENT QUERIES
        "data": [],              # THE DOCUMENTS NEEDED FOR THIS TEST
        "query": {               # THE Qb QUERY
            "from": "testdata"   # "testdata" WILL BE REPLACED WITH DATASTORE FILLED WITH data
        },
        "expecting": []          # THE EXPECTED RESULT (INCLUDING METADATA)
    },


    {
        "name": "singleton_alpha",
        "data": [
            {"a": "b"}
        ],
        "query": {
            "from": "testdata",
            "select": "a",
            "format": "cube"
        },
        "expecting": {
            "edges": [
                {
                    "name": "index",
                    "domain": {"type": "index", "min": 0, "max": 1, "interval": 1}
                }
            ],
            "cube": {
                "a": ["b"]
            }
        }
    },

    {
        "name": "singleton_alpha no select",
        "data": [
            {"a": "b"}
        ],
        "query": {
            "from": "testdata",
            "format": "table"
        },
        "expecting": {
            "header": ["a"],
            "data": ["b"]
        }
    },

    {
        "name": "singleton_alpha values",
        "data": ["a", "b"],
        "query": {
            "from": "testdata"
        },
        "expecting": ["a", "b"]
    },

    {
        "name": "select * from list of objects to cube",
        "data": [
            {"a": "b"},
            {"a": "d"}
        ],
        "query": {
            "from": "testdata",
            "select": "*",
            "format": "cube"
        },
        "expecting_list": [
            {"a": "b"},
            {"a": "d"}
        ],
        "expecting_table": {
            "header": ["a"],
            "data": [
                ["b"],
                ["d"]
            ]
        },
        "expecting_cube": {
            "edges": [
                {
                    "name": "index",
                    "domain": {"type": "index", "min": 0, "max": 2, "interval": 1}
                }
            ],
            "cube": {
                "a": ["b", "d"]
            }
        }
    },


    {
        "name": "select into children to table",
        "metadata": {
            "properties": {
                "x": {"type": "integer"},
                "a": {
                    "type": "nested",
                    "properties": {
                        "y": {
                            "type": "string"
                        },
                        "b": {
                            "type": "nested",
                            "properties": {
                                "c": {"type": "integer"},
                                "1": {"type": "integer"}

                            }
                        },
                        "z": {
                            "type": "string"
                        }
                    }
                }
            }
        },
        "data": [
            {"x": 5},
            {
                "a": [
                    {
                        "b": {"c": 13},
                        "y": "m"
                    },
                    {
                        "b": [
                            {"c": 17, "1": 27},
                            {"c": 19}

                        ],
                        "y": "q"
                    },
                    {
                        "y": "r"
                    }
                ],
                "x": 3
            },
            {
                "a": {"b": {"c": 23}},
                "x": 7
            },
            {
                "a": {"b": [
                    {"c": 29, "1": 31},
                    {"c": 37, "1": 41},
                    {"1": 47},
                    {"c": 53, "1": 59}
                ]},
                "x": 11
            }
        ],
        "query": {
            "from": "testdata.a.b",
            "select": ["...x", "c"]
        },
        "expecting_list": [
            {"x": 5, "c": None},
            {"x": 3, "c": 13},
            {"x": 3, "c": 17},
            {"x": 3, "c": 19},
            {"x": 7, "c": 23},
            {"x": 11, "c": 29},
            {"x": 11, "c": 37},
            {"x": 11, "c": None},
            {"x": 11, "c": 53}
        ],
        "expecting_table": {
            "header": ["x", "c"],
            "data": [
                [5, None],
                [3, 13],
                [3, 17],
                [3, 19],
                [7, 23],
                [11, 29],
                [11, 37],
                [11, None],
                [11, 53]
            ]
        },
        "expecting_cube": {
            "edges": [
                {
                    "name": "index",
                    "domain": {"type": "index", "min": 0, "max": 9, "interval": 1}
                }
            ],
            "cube": {
                "x": [5, 3, 3, 3, 7, 11, 11, 11, 11],
                "c": [None, 13, 17, 19, 23, 29, 37, None, 53]
            }
        }
    },


]
