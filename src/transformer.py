import pandas as pd


def players_transformer():
    players_pd = pd.read_csv("../staging/extracted/players.csv")
    renames = players_pd.groupby("Player")["AllName"].apply(", ".join).to_frame().reset_index()
    main = players_pd.drop_duplicates(subset=["Player"], keep="first").reset_index(drop=True)
    main = pd.merge(main, renames, on="Player", how="left").drop(columns=["AllName_x"]).rename(columns={"AllName_y": "AllName"})
    main["IsRetired"] = main["IsRetired"].astype(bool)
    main["ToWildrift"] = main["ToWildrift"].astype(bool)
    main = main.replace([pd.NaT], [None])
    return main
def tournaments_transformer():
    tournaments = pd.read_csv("../staging/extracted/tournaments.csv")
    tournaments["DateStart"] = pd.to_datetime(tournaments["DateStart"])
    tournaments["Date"] = pd.to_datetime(tournaments["Date"])
    major_regions =[
        "European League of Legends Championship Series", #EU LCS
        "North American League of Legends Championship Series", #NA LCS,
        "League of Legends Championship Series", #LCS
        "League Championship Series", #LCS
        "LoL EMEA Championship", #LEC
        "LoL Champions Korea", #LCK
        "Tencent LoL Pro League", #LPL
        "Mid-Season Invitational", #MSI
        "World Championship", #Worlds
    ]
    tournaments = tournaments[tournaments["League"].isin(major_regions)]
    filtered_TCL = tournaments[~tournaments["League"].str.contains("TCL")]
    return filtered_TCL

def tournamentresults_transformer(major_regions):
    tournamentresults_pd = pd.read_csv("../staging/extracted/tournamentresults.csv")
    filtered_tournamentresults = tournamentresults_pd[tournamentresults_pd["OverviewPage"].isin(major_regions)]
    filtered_TCL = filtered_tournamentresults[~filtered_tournamentresults["OverviewPage"].str.contains("TCL")]
    return filtered_TCL

def scoreboardgames_transformer(major_regions):
    scoreboardgames_pd = pd.read_csv("../staging/extracted/scoreboardgames.csv")
    filtered_scoreboardgames = scoreboardgames_pd[scoreboardgames_pd["OverviewPage"].isin(major_regions)]
    return filtered_scoreboardgames

def teams_transformer():
    teams_pd = pd.read_csv("../staging/extracted/teams.csv",dtype={"IsDisbanded":bool})
    return teams_pd

def main():
    tournaments = tournaments_transformer()
    tournaments.to_csv("../staging/transformed/tournaments.csv", index=False)

    players = players_transformer()
    players.to_csv("../staging/transformed/players.csv", index=False)

    tournamentresults = tournamentresults_transformer(tournaments["OverviewPage"])
    tournamentresults.to_csv("../staging/transformed/tournamentresults.csv", index=False)

    scoreboardgames = scoreboardgames_transformer(tournaments["OverviewPage"])
    scoreboardgames.to_csv("../staging/transformed/scoreboardgames.csv", index=False)

    teams = teams_transformer()
    teams.to_csv("../staging/transformed/teams.csv", index=False)


if __name__ == "__main__":
    main()