# ETL-Leaguepedia
 ETL process using Python, Leaguepedia API and PostgreSQL


## Introduction
This project is an ETL process that extracts data from the Leaguepedia API, transforms it and loads it into a PostgreSQL database. The data extracted is the information about the players, teams and every major tournament in the League of Legends competitive scene such as LCS, LEC, LCK,....

## Data Extraction
### Source: 
[Leaguepedia API](https://lol.fandom.com/wiki/Help:Leaguepedia_API)

## Tables extracted
- Players
- Teams
- ScoreboardGames
- Tournaments
- TournamentResults

## Data Transformation
### Tournaments & TournamentResults & ScoreboardGames
Use only major tournaments from major regions and international tournaments.

### Players
Merge multiple names of the same player into one.

## Data Loading
### Database
PostgreSQL

### ERD
![ERD](/ERD.png)






