# GitHub Open-Source Community Toxicity Analysis

A large-scale NLP pipeline for analyzing toxicity patterns and trends in open-source communities using fine-tuned language models.

## Project Overview

This project analyzes toxicity dynamics across 5 major software development domains (ML, DevOps, Frontend, Mobile, Game) over a 6-year period (2019-2024), processing **300M+ GitHub code review comments** extracted from **2TB+ of GH Archive data**.

### Key Objectives

- Build a high-performance data pipeline to ingest and transform massive GH Archive datasets
- Fine-tune and evaluate multiple pre-trained language models for toxicity detection
- Identify temporal toxicity trends and patterns across different development communities
- Assess the impact of external events (e.g., COVID-19) on community toxicity levels

## Technical Stack

### Data Processing
- **DuckDB**: Columnar SQL engine for efficient 2TB+ data ingestion and transformation
- **Apache Parquet**: Optimized storage format for structured comment data
- **Python**: ETL pipeline orchestration (NumPy, Pandas)

### Machine Learning
- **Hugging Face Transformers**: Model fine-tuning and inference
- **Models Evaluated**: BERT, RoBERTa, DistilBERT, [ToxiCR](https://github.com/WSU-SEAL/ToxiCR) (domain-specific toxicity detector for code reviews)

### Infrastructure
- **Docker**: Reproducible environment (`ghcr.io/lrrtuising/toxicr`)
- **Batch Processing**: Scalable inference on 300M+ comments

## Dataset Pipeline

### 1. Data Ingestion (`download.py`)
- Downloads raw data from [GH Archive](https://www.gharchive.org/)
- Filters relevant events (IssueCommentEvent, PullRequestReviewCommentEvent)

### 2. Data Transformation (`duckDB.sql`)
- Executes optimized SQL queries to extract 300M+ comments
- Categorizes repositories by domain using keyword/topic matching
- Outputs structured Parquet files per domain and year

### 3. Repository Scraping (`scraper/`)
- `MLScraper.py`, `devOpsScraper.py`, etc.: Domain-specific repo collectors
- Enriches dataset with repository metadata

### 4. Toxicity Scoring (`toxicity_scorer_toxicr.py`)
- Applies fine-tuned models to generate toxicity scores
- Batch processing optimized for throughput

## Model Evaluation

Located in `evaluation/`, this project benchmarks multiple models:

| Model | Dataset | Notebook |
|-------|---------|----------|
| BERT | Generic + GitHub-specific | `bert.ipynb`, `bert_github.ipynb` |
| RoBERTa | Generic + GitHub-specific | `RoBERTa.ipynb`, `RoBERTa_github.ipynb` |
| DistilBERT | Generic + GitHub-specific | `DistilBERT.ipynb`, `DistilBERT_github.ipynb` |
| ToxiCR | GitHub code reviews | `ToxiCR_github.ipynb` |
| Baseline | Traditional ML | `baseline.ipynb`, `baseline_github.ipynb` |

Fine-tuning on domain-specific GitHub data significantly improved model performance for toxicity detection in code review contexts.

## Analysis

The `analysis/` folder contains time-series analysis notebooks for each domain:
- Toxicity trend visualization (2019-2024)
- Statistical analysis of temporal patterns
- Correlation with external events (pandemic, major releases, etc.)

## Deployment

### Docker Container

Pre-built environment available:
```bash
docker pull ghcr.io/lrrtuising/toxicr:latest
docker run -it ghcr.io/lrrtuising/toxicr
```

### Local Setup

**Note**: Data files (~240MB Parquet) are excluded from this repository. Contact for access or regenerate using `download.py` + `duckDB.sql`.

```bash
# Install dependencies
pip install -r requirements.txt

# Run toxicity scoring
python toxicity_scorer_toxicr.py --input <comment_file> --output <score_file>

# Analyze results
jupyter notebook analysis/analysis_ml.ipynb
```

## Project Structure

```
.
├── README.md                    # This file
├── download.py                  # GH Archive downloader
├── duckDB.sql                   # SQL queries for data filtering
├── toxicity_scorer_toxicr.py    # Toxicity scoring script
├── scraper/                     # Domain-specific repo scrapers
│   ├── MLScraper.py
│   ├── devOpsScraper.py
│   ├── frontendScraper.py
│   ├── gameScraper.py
│   └── mobileScraper.py
├── evaluation/                  # Model evaluation notebooks
│   ├── bert.ipynb
│   ├── RoBERTa.ipynb
│   ├── DistilBERT.ipynb
│   ├── ToxiCR_github.ipynb
│   └── baseline.ipynb
├── analysis/                    # Time-series analysis per domain
│   ├── analysis_ml.ipynb
│   ├── analysis_devops.ipynb
│   ├── analysis_frontend.ipynb
│   ├── analysis_game.ipynb
│   └── analysis_mobile.ipynb
└── data/                        # Parquet files (excluded from repo)
    ├── repo/                    # Repository metadata
    └── score/                   # Comments + toxicity scores
```

## Results

- **Data Scale**: 2TB+ raw GH Archive → 300M curated comments
- **Domains Analyzed**: ML, DevOps, Frontend, Mobile, Game
- **Timespan**: 6 years (2019-2024)
- **Key Insight**: Toxicity levels showed temporal variations with spikes correlating to external events (e.g., early 2020) and domain-specific patterns

## Academic Context

This project was developed as part of a Master's capstone project at Northeastern University (2024-2025). It demonstrates:
- Large-scale data engineering on real-world datasets
- Applied NLP with state-of-the-art transformers
- Domain-specific model fine-tuning
- Time-series analysis and visualization

## Acknowledgments

This project uses [ToxiCR](https://github.com/WSU-SEAL/ToxiCR), a supervised learning-based tool developed by the Software Engineering Analytics Lab (SEAL) at Wayne State University for identifying toxic code review comments.

**ToxiCR Reference:**
- Sarker, J., Turzo, A. K., Dong, M., & Bosu, A. (2023). Automated Identification of Toxic Code Reviews Using ToxiCR. *ACM Transactions on Software Engineering and Methodology*. https://doi.org/10.1145/3583562

## Contact

- **Email**: ycyang.dev@gmail.com
- **GitHub**: [@lrrtuising](https://github.com/lrrtuising)
