from mwrogue.esports_client import EsportsClient
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



site = EsportsClient("lol")

def player_extractor():
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
    return players_pd

def scoreboardplayers_extractor():
    field = "SBP.OverviewPage, SBP.Name, SBP.Champion, SBP.Kills, SBP.Deaths, SBP.Assists, SBP.SummonerSpells, SBP.Gold, SBP.CS, SBP.PlayerWin, SBP.DateTime_UTC, SBP.Team, SBP.TeamVs, SBP.Role, SBP.Side"
    scoreboardplayers_pd = pd.DataFrame()
    for scoreboardplayers_batch in tqdm(range(0, extract_records("ScoreboardPlayers") + 500, 500)):
        scoreboardplayers = site.cargo_client.query(
        tables="ScoreboardPlayers=SBP",
        fields=field,
        limit=500,
        offset=scoreboardplayers_batch)
        scoreboardplayers_pd = pd.concat([pd.DataFrame(scoreboardplayers), scoreboardplayers_pd], ignore_index=True)
        time.sleep(2)
    return scoreboardplayers_pd

def scoreboardgames_extractor():
    field="SBG.OverviewPage, SBG.Team1, SBG.Team2, SBG.WinTeam, SBG.DateTime_UTC, SBG.Gamelength, SBG.Team1Bans, SBG.Team2Bans, SBG.Team1Picks, SBG.Team2Picks, SBG.Team1Players, SBG.Team2Players, SBG.MatchId, SBG.GameId"
    scoreboardplayers_pd = pd.DataFrame()
    for scoreboardgames_batch in tqdm(range(0, extract_records("ScoreboardGames") + 500, 500)):
        scoreboardgames = site.cargo_client.query(
        tables="ScoreboardGames=SBG",
        fields=field,
        limit=500,
        offset=scoreboardgames_batch)
        scoreboardplayers_pd = pd.concat([pd.DataFrame(scoreboardgames), scoreboardplayers_pd], ignore_index=True)
        time.sleep(1)
    return scoreboardplayers_pd
    
def tournaments_extractor():
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
    return tournamets_pd
    
def tournamentresults_extractor():
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
    return tournametresults_pd

def teams_extractor():
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
    return teams_pd

def main():
    players = player_extractor()
    print(players)
    players.to_csv("../staging/extracted/players.csv", index=False)

    # scoreboardplayers = scoreboardplayers_extractor()
    # print(scoreboardplayers)
    # scoreboardplayers.to_csv("scoreboardplayers.csv", index=False)

    scoreboardgames = scoreboardgames_extractor()
    print(scoreboardgames)
    scoreboardgames.to_csv("../staging/extracted/scoreboardgames.csv", index=False)

    tournaments = tournaments_extractor()
    print(tournaments.columns)
    tournaments.to_csv("../staging/extracted/tournaments.csv", index=False)

    tournamentresults = tournamentresults_extractor()
    print(tournamentresults)
    tournamentresults.to_csv("../staging/extracted/tournamentresults.csv", index=False)

    teams = teams_extractor()
    print(teams)
    teams.to_csv("../staging/extracted/teams.csv", index=False)

if __name__ == "__main__":
    # main()
    teams = teams_extractor()
    teams.to_csv("../staging/extracted/teams.csv", index=False)
