from krxdata.db.base import Schema


class SecurityMarket(Schema):
    def __init__(self):
        super().__init__()
        self.schema_name = "security_market"
        self.metadata.schema = self.schema_name

    def create_schema(self):
        super().create_schema(self.schema_name)

    def drop_schema(self, schema_name=None):
        super().drop_schema(self.schema_name)


