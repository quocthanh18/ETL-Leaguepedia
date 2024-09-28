from tqdm import tqdm
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time

def extract_records(tableName):
    BASE_URL = "https://lol.fandom.com/wiki/Special:CargoTables/"
    url = BASE_URL + tableName
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    re_ = re.compile(r"(\d+)")
    extracted_text = re_.findall(soup.find(property="og:description")["content"])
    return int("".join(extracted_text))


def player_extractor(**kwargs):
    site = kwargs.get("LoL")
    players_field = "P.Player, P.Country, P.Birthdate, P.ResidencyFormer, P.Team, P.Residency, P.Role, P.FavChamps, P.RoleLast, P.IsRetired, P.ToWildrift, PR.AllName"
    players_pd = pd.DataFrame()
    for players_batch in tqdm(range(0, extract_records("Players") + 500, 500)):
        players = site.cargo_client.query(
            tables="PlayerRedirects=PR, Players=P",
            fields=players_field,
            join_on="P.OverviewPage=PR.OverviewPage",
            where="P.Player IS NOT NULL",
            limit=500,
            offset=players_batch)
        players_pd = pd.concat([pd.DataFrame(players), players_pd], ignore_index=True)
    players_pd.to_csv(kwargs.get("dest") + "extracted/players.csv", index=False)
    return 

def scoreboardplayers_extractor(**kwargs):
    site = kwargs.get("LoL")
    field = "SBP.OverviewPage, SBP.Name, SBP.Champion, SBP.Kills, SBP.Deaths, SBP.Assists, SBP.SummonerSpells, SBP.Gold, SBP.CS, SBP.PlayerWin, SBP.DateTime_UTC, SBP.Team, SBP.TeamVs, SBP.Role, SBP.Side"
    scoreboardplayers_pd = pd.DataFrame()
    for scoreboardplayers_batch in tqdm(range(0, extract_records("ScoreboardPlayers") + 500, 500)):
        scoreboardplayers = site.cargo_client.query(
        tables="ScoreboardPlayers=SBP",
        fields=field,
        limit=500,
        offset=scoreboardplayers_batch)
        scoreboardplayers_pd = pd.concat([pd.DataFrame(scoreboardplayers), scoreboardplayers_pd], ignore_index=True)
        time.sleep(1)
    scoreboardplayers_pd.to_csv(kwargs.get("dest") + "extracted/scoreboardplayers.csv", index=False)
    return 

def scoreboardgames_extractor(**kwargs):
    site = kwargs.get("LoL")
    field="SBG.OverviewPage, SBG.Team1, SBG.Team2, SBG.WinTeam, SBG.DateTime_UTC, SBG.Gamelength, SBG.Team1Bans, SBG.Team2Bans, SBG.Team1Picks, SBG.Team2Picks, SBG.Team1Players, SBG.Team2Players, SBG.MatchId, SBG.GameId"
    scoreboardgames_pd = pd.DataFrame()
    for scoreboardgames_batch in tqdm(range(0, extract_records("ScoreboardGames") + 500, 500)):
        scoreboardgames = site.cargo_client.query(
        tables="ScoreboardGames=SBG",
        fields=field,
        limit=500,
        offset=scoreboardgames_batch)
        scoreboardgames_pd = pd.concat([pd.DataFrame(scoreboardgames), scoreboardgames_pd], ignore_index=True)
        time.sleep(1)
    scoreboardgames_pd.to_csv(kwargs.get("dest") + "extracted/scoreboardgames.csv", index=False)
    return 
    
def tournaments_extractor(**kwargs):
    site = kwargs.get("LoL")
    field="T.OverviewPage, T.DateStart, T.Date, T.Region, T.Country, T.EventType, T.League"
    tournamets_pd = pd.DataFrame()
    for tournament in tqdm(range(0, extract_records("Tournaments") + 500, 500)):
        tournaments = site.cargo_client.query(
        tables="Tournaments=T",
        fields=field,
        limit=500,
        offset=tournament)
        tournamets_pd = pd.concat([pd.DataFrame(tournaments), tournamets_pd], ignore_index=True)
        time.sleep(1)
    tournamets_pd.to_csv(kwargs.get("dest") + "extracted/tournaments.csv", index=False)
    return 
    
def tournamentresults_extractor(**kwargs):
    site = kwargs.get("LoL")
    fields="TR.OverviewPage, TR.Prize_USD, TR.Place, TR.Team"
    tournametresults_pd = pd.DataFrame()
    for result in tqdm(range(0, extract_records("TournamentResults") + 500, 500)):
        tournamentresults = site.cargo_client.query(
        tables="TournamentResults=TR",
        fields=fields,
        limit=500,
        offset=result)
        tournametresults_pd = pd.concat([pd.DataFrame(tournamentresults), tournametresults_pd], ignore_index=True)
        time.sleep(1)
    tournametresults_pd.to_csv(kwargs.get("dest") + "extracted/tournamentresults.csv", index=False)
    return 

def teams_extractor(**kwargs):
    site = kwargs.get("LoL")
    fields="T.OverviewPage, T.Short, T.Location, T.Region, T.IsDisbanded, T.RenamedTo"
    teams_pd = pd.DataFrame()
    for team in tqdm(range(0, extract_records("Teams") + 500, 500)):
        teams = site.cargo_client.query(
        tables="Teams=T",
        fields=fields,
        limit=500,
        offset=team)
        teams_pd = pd.concat([pd.DataFrame(teams), teams_pd], ignore_index=True)
        time.sleep(1)
    teams_pd.to_csv(kwargs.get("dest") + "extracted/teams.csv", index=False)
    return 

# def main():
#     players = player_extractor()
#     players.to_csv("/home/quocthanh/ETL-Leaguepedia/staging/extracted/players.csv", index=False)

#     # scoreboardplayers = scoreboardplayers_extractor()
#     # print(scoreboardplayers)
#     # scoreboardplayers.to_csv("scoreboardplayers.csv", index=False)

#     scoreboardgames = scoreboardgames_extractor()
#     scoreboardgames.to_csv("/home/quocthanh/ETL-Leaguepedia/staging/extracted/scoreboardgames.csv", index=False)

#     tournaments = tournaments_extractor()
#     tournaments.to_csv("../staging/extracted/tournaments.csv", index=False)

#     tournamentresults = tournamentresults_extractor()
#     tournamentresults.to_csv("../staging/extracted/tournamentresults.csv", index=False)

#     teams = teams_extractor()
#     teams.to_csv("../staging/extracted/teams.csv", index=False)

# if __name__ == "__main__":
#     main()
