from pyspark.sql import Column
from pyspark.sql.functions import *


class Field():
    def __init__(self, name):
        self.name = name

    def column(self) -> Column:
        return col(self.name)
