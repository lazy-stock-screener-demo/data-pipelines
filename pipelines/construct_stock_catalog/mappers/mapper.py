class ParsMapper:
    @staticmethod
    def toPersistence(report, cik):
        storage_key = dict()
        for i, (k, v) in enumerate(report.items()):
            normalized_key = k.lower().replace(" ", "_").replace("-", "_")
            storage_key["ticker"] = cik
            storage_key[normalized_key] = [
                {"year": year, "value": v[year]} for year in v
            ]
        return storage_key
