from mongoengine import *
from pipelines.construct_stock_catalog.mappers.mapper import ParsMapper
from pipelines.construct_stock_catalog.infra.mongoEngine.schema.CatalogCollection import (
    CatalogCollection,
)


class CatalogRepo(Document):
    def __init__(self):
        pass

    def save(self, report, cik):
        print("report", report)
        rawData = ParsMapper.toPersistence(report, cik)
        print("rawData", rawData)
        parsDoc = CatalogCollection(**rawData)
        parsDoc.save()

    def saveBulk(self, reports):
        for report in reports:
            self.save(report)

    def readBulk(self):
        return CatalogCollection.objects.all()


def catalogRepo():
    return CatalogRepo()