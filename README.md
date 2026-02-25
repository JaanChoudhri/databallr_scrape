# 🏀 NBA Data Scraping & Analysis with Databallr

This project demonstrates how to programmatically retrieve, clean, and
analyze advanced NBA statistics using the **Databallr API**.

The notebook walks through pulling offensive, defensive, shooting, and
impact metrics, transforming the raw dataset into clean analytical
tables, and generating stat leaderboards.

------------------------------------------------------------------------

## 📌 Project Overview

Databallr provides highly granular NBA player metrics, including:

-   Basic box score stats\
-   Advanced impact metrics (DPM, RAPM)\
-   Shooting splits by zone\
-   Possession-level statistics\
-   Offensive & defensive archetypes

This notebook:

1.  Connects to the Databallr Supabase API\
2.  Retrieves player-level season data\
3.  Cleans and formats the dataset\
4.  Creates filtered stat views\
5.  Produces sortable leaderboard tables

------------------------------------------------------------------------

## 📦 Dependencies

``` python
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
```

Install requirements:

``` bash
pip install pandas numpy requests beautifulsoup4
```

------------------------------------------------------------------------

## 🔗 Data Source

Data is pulled directly from the Databallr API endpoint:

``` python
data_url = "https://api.databallr.com/api/supabase/player_stats_with_metrics?year=2026&playoffs=0&min_minutes=50&limit=500&order_by=dpm&order_direction=desc"
```

Parameters Used:

-   year=2026\
-   playoffs=0\
-   min_minutes=50\
-   limit=500\
-   order_by=dpm\
-   order_direction=desc

------------------------------------------------------------------------

## 🧹 Data Cleaning & Transformation

The notebook performs:

### Filtering

Removes low-minute players to reduce statistical noise (e.g., MPG \>
15).

### Column Selection by Category

Metrics are grouped into structured analytical views:

-   Basic Offensive Metrics (PPG, 3P%, 3PA, FT%, RPG, APG)\
-   Advanced Offensive Metrics (DPM, oDPM, dDPM, TS%, rTS%)\
-   Shooting Splits (At Rim, Short Mid, Long Mid, 3P)\
-   Defensive Metrics (Deflections, Blocks, Steals)\
-   Impact Metrics (RAPM multi-year metrics)

### Column Renaming

Verbose API column names are standardized for clean output tables.

------------------------------------------------------------------------

## 📊 Leaderboards & Sorting

Example leaderboards created:

-   Top 3-Point Shooters\
-   Top Rebounders\
-   Top Assist Leaders\
-   Top Impact Players (DPM / RAPM)

------------------------------------------------------------------------

## 📈 What This Project Demonstrates

-   API integration in Python\
-   JSON → Pandas DataFrame transformation\
-   Data cleaning & formatting\
-   Advanced metric analysis\
-   Building reusable stat views\
-   Creating leaderboard logic\
-   Filtering out low-sample noise

------------------------------------------------------------------------

## 🚀 How To Run

1.  Clone the repository\
2.  Install dependencies\
3.  Open the Jupyter Notebook\
4.  Run cells sequentially

Ensure you have internet access to retrieve API data.

------------------------------------------------------------------------

## 🔮 Potential Enhancements

-   Add data caching\
-   Build automated season refresh scripts\
-   Add visualizations (Matplotlib / Seaborn / Plotly)\
-   Convert notebook into reusable Python modules\
-   Build a Streamlit dashboard\
-   Add player comparison tools\
-   Develop predictive models

------------------------------------------------------------------------

## 📁 Repository Structure

    📦 nba-databallr-analysis
     ┣ 📓 Databallr Stats Scrape.ipynb
     ┣ 📄 README.md
     ┗ 📄 requirements.txt

------------------------------------------------------------------------

## ⚠️ Disclaimer

This project uses publicly accessible data from Databallr's API
endpoint.\
All data rights belong to Databallr.

------------------------------------------------------------------------

## ✍️ Author

**Jaan Choudhri**\
Data Analyst \| Basketball Analytics Enthusiast
