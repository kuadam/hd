import argparse


# TODO: implement quitting
# TODO: implement or "are you sure" prompt

class Params:
    def __init__(self):
        self.source = ""  # which technology
        self.database = ""  # which database
        self.operation = ""
        self.table = ""
        self.column = ""
        self.value = ""

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
        self.parser.add_argument('-db', '--database', help="Database, topic or keyspace to be used. ")
        self.parser.add_argument('-o', '--operation', help="Operation to be compared. "
                                                           "Please check the documentation "
                                                           "for more info.", choices=self.possible_operations)
        self.parser.add_argument('-t', '--table', help="Table to be grouped by or search by value. "
                                                       "Join operation requires two tables."
                                                       "Insert them with semicolon as separator, ex: table1;table2")
        self.parser.add_argument('-c', '--column', help="Column to be search or aggregated by."
                                                        "Join operation requires two columns. "
                                                        "Insert them with semicolon as separator, ex: col1;col2")
        self.parser.add_argument('-v', '--value', help="Value to be search by or aggregated by.")
        self.parser.parse_args(self.args, namespace=self.params)

    # TODO(DONE): transformed into @staticmethod, default values to None otherwise error concerning parsing list to str
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
        if self.params.source == "cassandra":
            param_name = "keyspace"
        elif self.params.source == "kafka":
            param_name = "topic"
        while self.params.database == "":
            self.params.database = self.ask_about(param_name)

    # FIXME: case of user passing parameters not required for chosen operation --> inform that they will be ignored
    #                                                                             and clear values in class instance
    #  specific problems below:
    #  op find --> check if passed value is of correct type ex. deviceid must be int
    #  op max, min, avg, sum --> aggregation value should be empty str & lacking name of column to apply chosen op on
    #  op join --> aggregation value should be empty str & case of passing tab1; the second table is empty str, same
    #              problem with columns

    def get_missing_info_for_operation(self):
        while self.params.operation == "":
            self.params.operation = self.ask_about("operation", possible_values=self.possible_operations)
        while self.params.operation not in self.possible_operations:
            self.params.operation = self.ask_about("operation", missing=False, possible_values=self.possible_operations)
        if self.params.operation in ("max", "min", "avg", "sum"):
            while self.params.table == "":
                self.params.table = self.ask_about("table")
            while self.params.column == "":
                self.params.column = self.ask_about("group_by column")
            while self.params.value == "":
                self.params.value = self.ask_about("aggregated value")
            return 0
        elif self.params.operation == "find":
            while self.params.table == "":
                self.params.table = self.ask_about("table")
            while self.params.column == "":
                self.params.column = self.ask_about("column")
            while self.params.value == "":
                self.params.value = self.ask_about("wanted value")
            return 0
        else:  # join
            while self.params.table == "":
                self.params.table = self.ask_about("tables")
            self.params.table = self.params.table.split(";")
            while len(self.params.table) != 2:
                # TODO: add one table
                self.params.table = self.ask_about("tables", msg="You need two tables to do join! "
                                                                 "Enter table names separated by semicolon")
                self.params.table = self.params.table.split(";")
            while self.params.column == "":
                self.params.column = self.ask_about("columns")
            self.params.column = self.params.column.split(";")
            while len(self.params.column) != 2:
                self.params.column = self.ask_about("columns", msg="You need two columns to do join! "
                                                                   "Enter column names separated by semicolon")
                self.params.column = self.params.column.split(";")
            return 0

    def get_missing_info(self):
        self.get_missing_info_for_source()
        self.get_missing_info_for_operation()
