from .main_as_task import build_pars_func
from .step3_scrape_filing_summary_file import scrape_filing_summary_file
from pipelines.shared.utils import decorator_config


def test_step3_scrape_filing_summary_file(pars):
    filing_summary_url = {
        "url": "https://www.sec.gov/Archives/edgar/data/789019/000156459020034944/FilingSummary.xml",
    }
    decorator_config(scrape_filing_summary_file)(build_pars_func, filing_summary_url)


def run(pars):
    test_step3_scrape_filing_summary_file(pars)


def main():
    input_source = "local"
    run(dict(input_source=input_source))