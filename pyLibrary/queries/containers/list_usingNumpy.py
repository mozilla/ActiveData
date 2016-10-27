from pyLibrary.debugs.logs import Log


class List_usingNumpy(object):


    @classmethod
    def new_instance(cls, list):
        data = {}
        num_rows = len(list)
        for rownum, value in enumerate(list):
            for k, v in value.items():
                column = data.get(v)
                if column is None:
                    data[k] = Column(v, num_rows)
                column[rownum] = v

    def window(self, value, edges, sort):
        pass
        # {"rows":1}  # IF IN THE CONTEXT THAT HAS `rows` AND `rownum` FIELDS, THEN THIS IS rows[rownum+n]
        # {"rows":{<field>: offset}} =>  rows[rownum+offset].field

        # "window": {
        #     "name": "expire",
        #     "value": CODE("coalesce(rows[rownum+1].timestamp, Date.eod())"), {"coalesce": [{"rows": {"timestamp": 1}}, {"eod": {}}]}
        #     "edges": ["availability_zone", "instance_type"],
        #     "sort": "timestamp"
        # }


        # GROUP BY edges
        # SORT EDGES, STRINGS ARE RECURSIVE SORTING

        # SORT EACH IN CUBE

        # CALCULATE VALUE





"""
COLUMNS ARE REQUIRED TO HIDE DEALING WITH Nones AND STRINGS
"""

class Column(object):

    def __new__(cls, example, length):
        if isinstance(example, basestring):
            return StringColumn(length)
        else:
            Log.error("unknown type")


PREFIX_LENGTH = 8


class StringColumn(Column):

    def __init__(self, length, *args):
        object.__init__(self)

        self.max_length = 0
        self.data = [None]*length
        self.length = [0]*length
        self.raw = [None]*length

    def __getitem__(self, item):
        return self.data[item]

    def __setitem__(self, key, value):
        l = len(value)
        self.max_length = max(self.max_length, l)
        if l < PREFIX_LENGTH:
            self.data[key] = value + ("\0x00" * (PREFIX_LENGTH - l))
        else:
            self.data[key] = value[0:PREFIX_LENGTH]
        self.raw[key] = value

class SetColumn(Column):

    def __init__(self, length, *args):
        object.__init__(self)

        self.max_length = 0
        self.cardinality = 0
        self.unique = {}
        self.data = [None]*length

    def __getitem__(self, item):
        return self.unique[self.data[item]]

    def __setitem__(self, key, value):
        index = self.unique.get(key)
        if index is None:
            self.unique[value]=self.cardinality
            self.cardinality+=1
        self.data[key]=index

