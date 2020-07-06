import argparse

# TODO: implement quitting
# TODO: implement or "are you sure" prompt
# TODO: inform that unneeded parameters will be ignored and clear values in class instance


class Params:
    def __init__(self):
        self.source = ""  # which technology
        self.database = ""  # which database
        self.operation = ""
        self.table = ""
        self.column = ""
        self.value = ""
        self.aggregated = ""
        self.json_schema = ""
        self.join_version = 0

    def print(self):
        attrs = vars(self)
        print("Params:")
        print('\n'.join("%s: %s" % item for item in attrs.items()))


class InputData:
    def __init__(self, args):
        self.possible_operations = ["find", "join", "max", "min", "avg", "sum"]
        self.possible_sources = ["kafka", "cassandra", "mongoDB", "sqlServer"]
        self.args = args
        self.params = Params()
        self.parser = argparse.ArgumentParser(
            description="Compare computing time for operation done in ETL and Push-down modes.")

    def parse_arguments(self):
        self.parser.add_argument('-s', '--source', help="Technology to be used.", choices=self.possible_sources)
        self.parser.add_argument('-db', '--database', help="Database (mongoDB, SQLServer) or keyspace (Cassandra) to be used."
                                                           "For kafka, join operation requires two topics"
                                                           "Insert them with semicolon as separator, ex: topic1;topic2")
        self.parser.add_argument('-o', '--operation', help="Operation to be compared. "
                                                           "Please check the documentation "
                                                           "for more info.", choices=self.possible_operations)

        self.parser.add_argument('-t', '--table', help="Table or topic (Kafka) to group by or search by value. "
                                                       "Join operation requires two tables. "
                                                       "Insert them with semicolon as separator, ex: table1;table2")
        self.parser.add_argument('-c', '--column', help="Column to search or group by. "
                                                        "Join operation requires two columns. "
                                                        "Insert them with semicolon as separator, ex: col1;col2")
        self.parser.add_argument('-v', '--value', help="Value to search by."
                                                       "Insert one value or multiple values separated with comma")
        self.parser.add_argument('-a', '--aggregated', help="Value to aggregate by.")
        self.parser.add_argument('-j', '--json_schema', help="For kafka join operation requires two json schemas. "
                                                             "Insert them with semicolon as separator, ex: schema1;schema2"
                                 )
        self.parser.add_argument('-jv', '--join_version', help="For Cassadnra join operation is implemented in two versions."
                                                                "The default version (0) is for situation where only one (left) table is sorted."
                                                               " When both tables are sorted, insert 1 to choose the other version.",
                                                            type=int, default=0, choices=[0, 1])
        self.parser.parse_args(self.args, namespace=self.params)

    @staticmethod
    def ask_about(name, missing=True, possible_values=None, msg=None):
        if msg is not None:
            print(msg)
        if possible_values is None:
            possible_values = ""
        else:
            possible_values = "\nPossible values are:\n" + "\n".join(possible_values)
        if missing:
            return input(("Parameter {} needed" + possible_values + "\n:").format(name))
        else:
            return input(("Wrong value for parameter {}" + possible_values + "\n:").format(name))

    def get_missing_info_for_source(self):
        while self.params.source == "":
            self.params.source = self.ask_about("source", possible_values=self.possible_sources)
        while self.params.source not in self.possible_sources:
            self.params.source = self.ask_about("source", missing=False, possible_values=self.possible_sources)
        param_name = "database"
        if self.params.source == "kafka":
            return
        elif self.params.source == "cassandra":
            param_name = "keyspace"
        while self.params.database == "":
            self.params.database = self.ask_about(param_name)

    # check if array contains empty string or string containing only whitespaces
    @staticmethod
    def empty_values(array):
        return True in [(string.isspace() or not string) for string in array]

    def get_missing_info_for_operation(self):
        while self.params.operation == "":
            self.params.operation = self.ask_about("operation", possible_values=self.possible_operations)
        while self.params.operation not in self.possible_operations:
            self.params.operation = self.ask_about("operation", missing=False, possible_values=self.possible_operations)
        if self.params.source == "kafka" and self.params.operation != "join":
            while self.params.json_schema == "":
                self.params.json_schema = self.ask_about("json schema")
        if self.params.operation in ("max", "min", "avg", "sum"):
            while self.params.table == "":
                self.params.table = self.ask_about("table")
            while self.params.column == "":
                self.params.column = self.ask_about("column to group by")
            while self.params.aggregated == "":
                self.params.aggregated = self.ask_about("aggregated value")
            return 0
        elif self.params.operation == "find":
            while self.params.table == "":
                param_name = "table "
                if self.params.source == "kafka":
                    param_name = "topic"
                self.params.table = self.ask_about(param_name)
            while self.params.column == "":
                self.params.column = self.ask_about("column")
            while self.params.value == "":
                self.params.value = self.ask_about("wanted value")
            if self.params.limit is not None:
                while self.params.count in (None, ""):
                    self.params.count = self.ask_about("count")
                self.params.count = float(self.params.count)
            else:
                self.params.count = None
            return 0
        else:  # join
            if self.params.source=="kafka":
                while self.params.json_schema == "":
                    self.params.json_schema = self.ask_about("json schemas")
                self.params.json_schema = self.params.json_schema.split(";")
                while len(self.params.json_schema) != 2 or self.empty_values(self.params.json_schema):  # check if not empty
                    self.params.json_schema = self.ask_about("json schema", msg="You need two json schemas to do join! "
                                                                       "Enter json schema names separated by semicolon")
                    self.params.json_schema = self.params.json_schema.split(";")
            while self.params.table == "":
                param_name = "tables"
                if self.params.source == "kafka":
                    param_name = "topics"
                self.params.table = self.ask_about(param_name)
            self.params.table = self.params.table.split(";")
            while len(self.params.table) != 2 or self.empty_values(self.params.table):  # check if not empty
                param_name = "tables"
                if self.params.source == "kafka":
                    param_name = "topics"
                # TODO: add one table
                self.params.table = self.ask_about(param_name, msg="You need two "+param_name+" to do join! "
                                                                 "Enter "+param_name[:-1]+" names separated by semicolon")
                self.params.table = self.params.table.split(";")
            while self.params.column == "":
                self.params.column = self.ask_about("columns")
            self.params.column = self.params.column.split(";")
            while len(self.params.column) != 2 or self.empty_values(self.params.column):  # check if not empty
                self.params.column = self.ask_about("columns", msg="You need two columns to do join! "
                                                                   "Enter column names separated by semicolon")
                self.params.column = self.params.column.split(";")

            return 0

    def get_missing_info(self):
        self.get_missing_info_for_source()
        self.get_missing_info_for_operation()
