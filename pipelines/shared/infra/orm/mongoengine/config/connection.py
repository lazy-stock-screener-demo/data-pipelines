from mongoengine import connect, disconnect
import pipelines.construct_stock_catalog.config.env as env


def connectedMongo():
    connect(
        "stock-db", host=env.mongo_db_url + "/?authSource=stock-db", alias="stockdb1"
    )


def disconnectMongo():
    disconnect(alias="stockdb1")