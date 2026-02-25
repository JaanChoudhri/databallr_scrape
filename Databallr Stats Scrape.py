# Databricks notebook source
# MAGIC %md
# MAGIC #NBA Data Scraping Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ## Necessary Packages

# COMMAND ----------

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

# COMMAND ----------

# MAGIC %md
# MAGIC [Stats Glossary](https://databallr.com/stats/glossary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Retrieval

# COMMAND ----------

data_url = "https://api.databallr.com/api/supabase/player_stats_with_metrics?year=2026&playoffs=0&min_minutes=50&limit=500&order_by=dpm&order_direction=desc"
site_response = requests.get(data_url)

if site_response.status_code == 200:
    print(f"Successfully accessed the page: {data_url}")
else:
    print(f"Failed to retrieve page. Status code: {site_response.status_code}")    
    exit()

data=site_response.json()
print("\n")
print(f"Data type: {type(data)}")

allStats = pd.DataFrame(data)
display(allStats)


# COMMAND ----------

# MAGIC %md
# MAGIC ##Offense

# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic Metrics

# COMMAND ----------

# DBTITLE 1,Generate Per Game Stats


basicOffCols = ['Name', 'Pos2','Offensive Archetype', 'TeamAbbreviation',  'GamesPlayed', 'MPG', 'd_Points_PerGame', '3P_PERC', 'd_FG3A_PerGame', 'FT_PERC', 'd_Rebounds_PerGame', 'd_Assists_PerGame']
basicOffStats = allStats[basicOffCols][allStats['MPG']>15]  # filter out noise, role players
display(basicOffStats)


# COMMAND ----------

# MAGIC %md
# MAGIC Nice! We successfully returned our data. However, it looks a bit messy. Percentages aren't clean and there are too many decimals. Let's work towards cleaning up the data so it looks nice. 

# COMMAND ----------

# DBTITLE 1,Data Cleaning
basicOffStats = basicOffStats.rename( #Rename Columns to look like standard stat s
                    columns = {'TeamAbbreviation': 'Team',
                               'Pos2': 'Pos',
                               'd_Points_PerGame': 'PPG',
                               '3P_PERC': '_3P_Pct',
                               'd_FG3A_PerGame': '_3PA',
                               'FT_PERC': 'FT_Pct',
                               'd_Rebounds_PerGame' :'RPG',
                               'd_Assists_PerGame': 'APG',
}
                    )


basicOffStats = basicOffStats.assign(
    MPG = (basicOffStats['MPG']).round(1),
    PPG = (basicOffStats['PPG']).round(1),
    RPG = (basicOffStats['RPG']).round(1),
    APG = (basicOffStats['APG']).round(1),
    _3PA = (basicOffStats['_3PA']).round(1),
    FT_Pct = (basicOffStats['FT_Pct'] * 100).round(1).astype(str) + '%',
    _3P_Pct = (basicOffStats['_3P_Pct'] * 100).round(1).astype(str) + '%'    
)

display(basicOffStats)

# COMMAND ----------

# MAGIC %md
# MAGIC Looks better! Now, let's try and look at some stats leaders. 

# COMMAND ----------

# DBTITLE 1,Top 25 3PT Shooters
top3PT = display(
    basicOffStats[basicOffStats['_3PA'] > 3][['Name', 'Team', 'GamesPlayed', '_3P_Pct']] ##filter to only look at 3pFGA > 3, only return name, team, games played, 3p%
    .sort_values(by="_3P_Pct", ascending=False) # Sort by 3P% descending
    .head(25) # Onll return Top 25
    [['Name', 'Team', 'GamesPlayed', '_3P_Pct']]
)

top3PT

# COMMAND ----------

# DBTITLE 1,Top Rebounders
topRPG = basicOffStats[['Name', 'Team', 'GamesPlayed', 'RPG']].sort_values(
        by="RPG", ascending=False).head(10)[['Name', 'Team', 'GamesPlayed', 'RPG']] 

display(topRPG)

# COMMAND ----------

# DBTITLE 1,Top Assists
topAPG = basicStats[['Name', 'Team', 'GamesPlayed', 'APG']].sort_values(
        by="APG", ascending=False).head(10)[['Name', 'Team', 'GamesPlayed', 'APG']] 

display(topAPG)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advanced Metrics

# COMMAND ----------

# DBTITLE 1,Generate Stats + Data Cleaning
advOffCols = [
    "Name",
    "Pos2",
    "Offensive Archetype",
    "TeamAbbreviation",
    "GamesPlayed",
    "MPG",
    "dpm",
    "o_dpm",
    "d_dpm",
    "PASSING_on-ball-time%",
    "nonPassTOVPct",
    "TS_pct",
    "rTSPct",
    "d_FTA_Per100",
    "SelfORebPct",
    "TeammateMissORebPerc",
    "d_AtRimAssists_Per100"

]

advOffStats = allStats[advOffCols][allStats["MPG"] > 15]  # filter out noise, role players

advOffStats = advOffStats.rename(
    columns={
        "TeamAbbreviation": "Team",
        "Pos2": "Pos",
        "dpm": "DPM",
        "o_dpm": "O_DPM",
        "d_dpm": "D_DPM",
        "PASSING_on-ball-time%": "On_Ball_Time",
        "nonPassTOVPct": "sTOV",
        "rTSPct": "rTS_Pct",
        "d_FTA_Per100": "FTA_Per100",
        "d_AtRimAssists_Per100": "RimAst_Per100"
    }
)

advOffStats = advOffStats.assign(
    MPG = (advOffStats['MPG']).round(1),
    DPM = (advOffStats['DPM']).round(1),
    O_DPM = (advOffStats['O_DPM']).round(1),
    D_DPM = (advOffStats['D_DPM']).round(1),
    On_Ball_Time = (advOffStats['On_Ball_Time']).round(1).astype(str) + '%',
    sTOV = (advOffStats['sTOV']).round(1).astype(str) + '%',
    TS_pct = (advOffStats['TS_pct'] * 100).round(1).astype(str) + '%',
    rTS_Pct = (advOffStats['rTS_Pct']).round(1).astype(str) + '%',
    FTA_Per100 = advOffStats['FTA_Per100'].round(1),   
    SelfORebPct = (advOffStats['SelfORebPct']*100).round(1).astype(str)+'%',
    TeammateMissORebPerc = (advOffStats['TeammateMissORebPerc']*100).round(1).astype(str)+'%',
    RimAst_Per100 = advOffStats['FTA_Per100'].round(1)  
)


display(advOffStats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shooting

# COMMAND ----------

shootingCols = [
    "Name",
    "Pos2",
    "Offensive Archetype",
    "TeamAbbreviation",
    "GamesPlayed",
    "MPG",
    "d_AtRimFGA_PerGame",
    "RimFGPerc",
    "d_ShortMidRangeFGA_PerGame",
    "ShortMidFGPerc",
    "d_LongMidRangeFGA_PerGame",
    "LongMidFGPerc",
    "d_FG3A_PerGame",
    "3P_PERC",
    "d_FTA_PerGame",
    "FT_PERC"
]

shootingStats = allStats[shootingCols][(allStats["MPG"] > 15) & (allStats["GamesPlayed"] > 15)]  # filter out noise, role players

shootingStats = shootingStats.rename(
    columns={
        "TeamAbbreviation": "Team",
        "Pos2": "Pos",
        "d_AtRimFGA_PerGame": "Rim_ATT_PerGame",
        "RimFGPerc": "RimFG_Pct",
        "d_ShortMidRangeFGA_PerGame": "ShortMidRangeFGA_PerGame",
        "ShortMidFGPerc": "ShortMidFG_Pct",
        "d_LongMidRangeFGA_PerGame": "LongMidRangeFGA_PerGame",
        "LongMidFGPerc": "LongMidFG_Pct",
        "d_FG3A_PerGame": "_3PA_PerGame",
        "3P_PERC": "_3P_Pct",
        "d_FTA_PerGame": "FTA_PerGame",
        "FT_PERC": "FT_Pct"
    }
)

shootingStats = shootingStats.assign(
    MPG = (shootingStats['MPG']).round(1),
    Rim_ATT_PerGame = (shootingStats['Rim_ATT_PerGame']).round(1),
    RimFG_Pct = (shootingStats['RimFG_Pct']* 100).round(1).astype(str) + '%',
    ShortMidRangeFGA_PerGame = (shootingStats['ShortMidRangeFGA_PerGame']).round(1),
    ShortMidFG_Pct = (shootingStats['ShortMidFG_Pct']* 100).round(1).astype(str) + '%',
    LongMidRangeFGA_PerGame = (shootingStats['LongMidRangeFGA_PerGame']).round(1),
    LongMidFG_Pct = (shootingStats['LongMidFG_Pct']* 100).round(1).astype(str) + '%',    
    _3PA_PerGame = (shootingStats['_3PA_PerGame']).round(1),
    _3P_Pct = (shootingStats['_3P_Pct']* 100).round(1).astype(str) + '%',
    FTA_PerGame = (shootingStats['FTA_PerGame']).round(1),
    FT_Pct = (shootingStats['FT_Pct']* 100).round(1).astype(str) + '%'
)


display(shootingStats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### True Shooting

# COMMAND ----------

trueShootingCols = [
    "Name",
    "Pos2",
    "Offensive Archetype",
    "TeamAbbreviation",
    "GamesPlayed",
    "MPG",
    "d_AtRimFGA_PerGame",
    "RimFGPerc",
    "d_ShortMidRangeFGA_PerGame",
    "ShortMidFGPerc",
    "d_LongMidRangeFGA_PerGame",
    "LongMidFGPerc",
    "d_FG3A_PerGame",
    "3P_PERC",
    "d_FTA_PerGame",
    "FT_PERC"
]

trueShootingStats = allStats[trueShootingCols][(allStats["MPG"] > 15) & (allStats["GamesPlayed"] > 15)]  # filter out noise, role players

trueShootingStats = trueShootingStats.rename(
    columns={
        "TeamAbbreviation": "Team",
        "Pos2": "Pos",
        "d_AtRimFGA_PerGame": "Rim_ATT_PerGame",
        "RimFGPerc": "RimFG_Pct",
        "d_ShortMidRangeFGA_PerGame": "ShortMidRangeFGA_PerGame",
        "ShortMidFGPerc": "ShortMidFG_Pct",
        "d_LongMidRangeFGA_PerGame": "LongMidRangeFGA_PerGame",
        "LongMidFGPerc": "LongMidFG_Pct",
        "d_FG3A_PerGame": "_3PA_PerGame",
        "3P_PERC": "_3P_Pct",
        "d_FTA_PerGame": "FTA_PerGame",
        "FT_PERC": "FT_Pct"
    }
)

trueShootingStats = trueShootingStats.assign(
    MPG = (trueShootingStats['MPG']).round(1),
    Rim_ATT_PerGame = (trueShootingStats['Rim_ATT_PerGame']).round(1),
    RimFG_Pct = (trueShootingStats['RimFG_Pct']* 100).round(1).astype(str) + '%',
    ShortMidRangeFGA_PerGame = (trueShootingStats['ShortMidRangeFGA_PerGame']).round(1),
    ShortMidFG_Pct = (trueShootingStats['ShortMidFG_Pct']* 100).round(1).astype(str) + '%',
    LongMidRangeFGA_PerGame = (trueShootingStats['LongMidRangeFGA_PerGame']).round(1),
    LongMidFG_Pct = (trueShootingStats['LongMidFG_Pct']* 100).round(1).astype(str) + '%',    
    _3PA_PerGame = (trueShootingStats['_3PA_PerGame']).round(1),
    _3P_Pct = (trueShootingStats['_3P_Pct']* 100).round(1).astype(str) + '%',
    FTA_PerGame = (trueShootingStats['FTA_PerGame']).round(1),
    FT_Pct = (trueShootingStats['FT_Pct']* 100).round(1).astype(str) + '%'
)


display(trueShootingStats)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Defense

# COMMAND ----------

# DBTITLE 1,Data Collection + Cleaning
defCols = [
    "Name",
    "Pos2",
    "TeamAbbreviation",
    "GamesPlayed",
    "MPG",
    "d_dpm",
    "td_drapm",
    "td_dts",
    "td_dtov",
    "td_dreb",
    "d_Steals_PerGame",
    "d_OFFD_PerGame",
    "d_Blocks_PerGame",
    "d_FTOVs_PerGame",
    "d_FTOVs_Per100",
    "d_DEFLECTIONS_PerGame",
    "d_DEFLECTIONS_Per100",
    "DefFGReboundPct"

]

defensiveStats = allStats[defCols][(allStats["MPG"] > 15) & (allStats["GamesPlayed"] > 15)]  # filter out noise, role players

defensiveStats = defensiveStats.rename(
    columns={
        "TeamAbbreviation": "Team",
        "Pos2": "Pos",
        "d_dpm": "D_DPM",
        "td_drapm": "DRAPM",
        "td_dts": "dTS",
        "td_dtov": "dTOV",
        "td_dreb": "dREB",
        "d_Steals_PerGame": "STL_PerGame",
        "d_OFFD_PerGame": "OffensiveFoulsDrawn_PerGame",
        "d_Blocks_PerGame": "Blocks_PerGame",
        "d_FTOVs_PerGame": "Forced_TO_PerGame",
        "d_FTOVs_Per100":"Forced_TO_Per100",
        "d_DEFLECTIONS_PerGame": "Deflections_PerGame",
        "d_DEFLECTIONS_Per100": "Deflections_Per100"
    }
)

defensiveStats = defensiveStats.assign(
    MPG = (defensiveStats['MPG']).round(1),
    D_DPM = (defensiveStats['D_DPM']).round(1),
    DRAPM = (defensiveStats['DRAPM']).round(1),
    dTS = (defensiveStats['dTS']).round(1),
    dTOV = (defensiveStats['dTOV']).round(1),
    dREB = (defensiveStats['dREB']),
    STL_PerGame = defensiveStats['STL_PerGame'].round(1),   
    OffensiveFoulsDrawn_PerGame = (defensiveStats['OffensiveFoulsDrawn_PerGame']).round(1),
    Blocks_PerGame = (defensiveStats['Blocks_PerGame']*100).round(1),
    Forced_TO_PerGame = defensiveStats['Forced_TO_PerGame'].round(1),  
    Forced_TO_Per100 = defensiveStats['Forced_TO_Per100'].round(1),  
    Deflections_PerGame = defensiveStats['Deflections_PerGame'].round(1),  
    Deflections_Per100 = defensiveStats['Deflections_Per100'].round(1),      
)


display(defensiveStats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Impact

# COMMAND ----------

# DBTITLE 1,Data Collection + Cleaning
impactCols = [
    "Name",
    "Pos2",
    "TeamAbbreviation",
    "GamesPlayed",
    "MPG",
    "o_dpm",
    "d_dpm",
    "dpm",
    "two_year_orapm",
    "two_year_drapm",
    "two_year_rapm",
    "three_year_orapm",
    "three_year_drapm",
    "three_year_rapm"

]

impactStats = allStats[impactCols][(allStats["MPG"] > 15) & (allStats["GamesPlayed"] > 15)]  # filter out noise, role players

impactStats = impactStats.rename(
    columns={
        "TeamAbbreviation": "Team",
        "Pos2": "Pos",
        "o_dpm": "O_DPM",
        "d_dpm": "D_DPM",
        "dpm": "DPM",
        "two_year_orapm": "TwoYear_ORAPM",
        "two_year_drapm": "TwoYear_DRAPM",
        "two_year_rapm": "TwoYear_RAPM", 
        "three_year_orapm": "ThreeYear_ORAPM",
        "three_year_drapm": "ThreeYear_DRAPM",
        "three_year_rapm": "ThreeYear_RAPM",       


    }
)

impactStats = impactStats.assign(
    MPG = (impactStats['MPG']).round(1),
    D_DPM = (impactStats['D_DPM']).round(1),
    O_DPM = (impactStats['O_DPM']).round(1),
    DPM = (impactStats['DPM']).round(1),
    TwoYear_ORAPM = (impactStats['TwoYear_ORAPM']).round(1),
    TwoYear_DRAPM = (impactStats['TwoYear_DRAPM']).round(1),
    TwoYear_RAPM = impactStats['TwoYear_RAPM'].round(1),   
    ThreeYear_ORAPM = (impactStats['ThreeYear_ORAPM']).round(1),
    ThreeYear_DRAPM = (impactStats['ThreeYear_DRAPM']).round(1),
    ThreeYear_RAPM = impactStats['ThreeYear_RAPM'].round(1),     
)


display(impactStats)