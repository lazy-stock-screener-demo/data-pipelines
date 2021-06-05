import pandas as pd
from ..catalog_builder.ProfitBuilder import ProfitBuilder
from ..catalog_builder.SafetyBuilder import SafetyBuilder
from ..price_strategy.DefaultPriceStrategy import PriceStrategy
from ..price_strategy.DDMStrategy import DDMStrategy
from ..price_strategy.DDM2Strategy import DDM2Strategy
from ..price_strategy.StrategyContext import StrategyContext


def BuildCatalogCollection(**kwargs):
    result = []
    result.append(ProfitBuilder.create(**kwargs))
    result.append(SafetyBuilder.create(**kwargs))
    print(("{0}\n").format(result))
    return pd.concat(result, axis=1).fillna(0).T.to_dict("index")


def BuildPriceCollection(**kwargs):
    context = StrategyContext(PriceStrategy(**kwargs))
    context.append_strategy = DDMStrategy(**kwargs)
    context.append_strategy = DDM2Strategy(**kwargs)
    return context.calculate_estimated_stock_price().to_dict("index")


class CatalogCollectionFactory:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def create_collection(self):
        return BuildCatalogCollection(**self.kwargs)


class PriceCollectionFactory:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def create_collection(self):
        return BuildPriceCollection(**self.kwargs)


class CreateCollectionApp:
    def __init__(self, **kwargs):
        self._factory = kwargs.get("factory")

    def create(self):
        return self._factory.create_collection()
