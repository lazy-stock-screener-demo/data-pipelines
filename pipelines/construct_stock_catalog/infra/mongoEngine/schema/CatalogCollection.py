import uuid
from pipelines.shared.infra.orm.mongoengine.config.connection import (
    connectedMongo,
    disconnectMongo,
)
from mongoengine import (
    Document,
    UUIDField,
    StringField,
    FloatField,
    ListField,
    IntField,
    EmbeddedDocument,
    EmbeddedDocumentField,
)

connectedMongo()


class ValueFlow(EmbeddedDocument):
    year = IntField()
    value = FloatField()


class CatalogCollection(Document):
    stock_id = UUIDField(primary_key=True, required=True, default=uuid.uuid4())
    ticker = StringField(max_length=30, unique=False, required=True, null=False)

    operating_cash_flow = ListField(EmbeddedDocumentField(ValueFlow))
    net_income = ListField(EmbeddedDocumentField(ValueFlow))
    roe = ListField(EmbeddedDocumentField(ValueFlow))
    ros = ListField(EmbeddedDocumentField(ValueFlow))
    roa = ListField(EmbeddedDocumentField(ValueFlow))
    eps = ListField(EmbeddedDocumentField(ValueFlow))
    total_assets = ListField(EmbeddedDocumentField(ValueFlow))
    total_liabilities = ListField(EmbeddedDocumentField(ValueFlow))
    long_term_debt = ListField(EmbeddedDocumentField(ValueFlow))
    current_ratio = ListField(EmbeddedDocumentField(ValueFlow))
    free_cash_flow = ListField(EmbeddedDocumentField(ValueFlow))

    meta = {"collection": "catalog", "db_alias": "stockdb1"}

    def __repr__(self):
        return "<CatalogCollection(stock_id='%s', ticker='%s',operating_cash_flow='%s', net_income='%s')>" % (
            self.stock_id,
            self.ticker,
            self.operating_cash_flow,
            self.net_income,
        )


# disconnectMongo()