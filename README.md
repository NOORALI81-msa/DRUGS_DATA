# Geometric Crawler

A Python-based web crawling project focused on structured extraction, with support for:
- Scrapy spiders (general and drug-focused)
- self-healing/repair logic
- parallel crawling
- optional LLM-assisted repair
- Streamlit UI for running extraction workflows
- JSONL/CSV output organization by domain

## Project Structure

- `geometric_crawler/`: core package (settings, pipelines, repair, spiders)
- `geometric_crawler/spiders/`: spider implementations
- `outputs/`: crawl outputs grouped by domain/source
- `run.py`: main CLI runner for a single crawl
- `run_parallel.py`: multi-site parallel crawler
- `app.py`: Streamlit UI
- `requirements.txt`: pip dependencies
- `pyproject.toml`: project metadata

## Requirements

- Python 3.10+ recommended
- Windows, macOS, or Linux
- Playwright browser binaries (if using Playwright paths)
- Optional: MongoDB (for DB-related scripts)

> Note: `pyproject.toml` currently declares `>=3.14`, but many dependencies are commonly used on Python 3.10+.

## Setup

1. Create and activate a virtual environment.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies.

```powershell
pip install -r requirements.txt
```

3. Install Playwright browser binaries.

```powershell
playwright install
```

4. (Optional) Add environment variables in `.env` as needed.

## Run the Crawler (CLI)

Basic crawl:

```powershell
python run.py --spider geometric --url https://example.com --pages 50 --depth 2
```

Drug spider example:

```powershell
python run.py --spider drug --url https://www.1mg.com/drugs-all-medicines --pages 100 --depth 3 --max-drugs 200
```

Resume/append output example:

```powershell
python run.py --spider drug --url https://www.1mg.com/drugs-all-medicines --use-existing-file --existing-file outputs/drug/csv/drug_latest.csv
```

Useful flags:
- `--follow-patterns`: URL patterns to follow
- `--http-only`: skip Playwright and use HTTP-only
- `--playwright-always`: force Playwright on all pages
- `--use-llm`: enable LLM repair (provider/model configurable)
- `--detail-only`: write only detailed page entries

## Run Parallel Crawls

Preset example:

```powershell
python run_parallel.py --preset drugs --cores 4 --pages 50 --depth 2
```

Custom URLs example:

```powershell
python run_parallel.py --urls https://site1.com https://site2.com --cores 2 --pages 30
```

## Run Streamlit UI

```powershell
streamlit run app.py
```

The UI provides:
- quick extraction helpers
- spider run controls
- provider/model settings for LLM-backed flows
- live run/output visibility

## Outputs and Logs

- Crawl outputs are written under `outputs/` (domain/source specific folders).
- Run state/job directories are stored in `.crawl_state/`.
- Crawler logs are present at repository root as `crawler_log_*.txt`.

## Common Troubleshooting

- `scrapy` command not found: ensure virtual environment is active and dependencies are installed.
- Playwright issues: run `playwright install` again.
- MongoDB connection errors: start MongoDB service locally if using DB scripts.
- Slow crawling: tune `--threads`, `--delay`, and HTTP/Playwright mode flags.

## License

No license file is currently included in this repository.
