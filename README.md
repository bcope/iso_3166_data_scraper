# ISO 3166 Data Scraper

This utility script scrapes the [ISO 3166-1 data from the Wikipedia page](https://en.wikipedia.org/wiki/ISO_3166-1) and then attempts to scrape the ISO 3166-2 country subdivision data from each specific country page.

The column names are cleaned and normalized using constants (although none are yet specified).

The results are saved to a JSON file.

## Requirements

- Python 3.7+

## Run the code

Optionally use the `--output-file-path` and `--debug` flags. Use `--help` for more info.

```bash
python main.py
```
