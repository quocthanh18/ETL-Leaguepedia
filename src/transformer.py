import pandas as pd
from io import BytesIO

def players_transformer(**kwargs):
    s3 = kwargs.get("s3")
    obj = s3.get_key('extracted/players.csv', 'leaguepedia')
    read_buffer = BytesIO()
    obj.download_fileobj(read_buffer)
    read_buffer.seek(0)
    players_pd = pd.read_csv(read_buffer)
    renames = players_pd.groupby("Player")["AllName"].apply(", ".join).to_frame().reset_index()
    main = players_pd.drop_duplicates(subset=["Player"], keep="first").reset_index(drop=True)
    main = pd.merge(main, renames, on="Player", how="left").drop(columns=["AllName_x"]).rename(columns={"AllName_y": "AllName"})
    main["IsRetired"] = main["IsRetired"].astype(bool)
    main["ToWildrift"] = main["ToWildrift"].astype(bool)
    main = main.replace([pd.NaT], [None])
    write_buffer = BytesIO()
    main.to_csv(write_buffer, index=False, mode='wb', encoding='utf-8')
    write_buffer.seek(0)
    s3.load_file_obj(write_buffer, bucket_name='leaguepedia', key='transformed/players.csv')
    return 
def tournaments_transformer(**kwargs):
    # tournaments = pd.read_csv("../staging/extracted/tournaments.csv")
    s3 = kwargs.get("s3")
    obj = s3.get_key('extracted/tournaments.csv', 'leaguepedia')
    read_buffer = BytesIO()
    obj.download_fileobj(read_buffer)
    read_buffer.seek(0)
    tournaments = pd.read_csv(read_buffer)
    # tournaments = pd.read_csv(kwargs.get("dest") + "extracted/tournaments.csv")
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
    write_buffer = BytesIO()
    # filtered_TCL.to_csv(kwargs.get("dest") + "transformed/tournaments.csv", index=False)
    filtered_TCL.to_csv(write_buffer, index=False, mode='wb', encoding='utf-8')
    write_buffer.seek(0)
    s3.load_file_obj(write_buffer, bucket_name='leaguepedia', key='transformed/tournaments.csv')
    return 

def tournamentresults_transformer(**kwargs):
    # tournamentresults_pd = pd.read_csv("../staging/extracted/tournamentresults.csv")
    s3 = kwargs.get("s3")
    obj = s3.get_key('extracted/tournamentresults.csv', 'leaguepedia')
    read_buffer = BytesIO()
    obj.download_fileobj(read_buffer)
    read_buffer.seek(0)
    tournamentresults_pd = pd.read_csv(read_buffer)
    major_regions = pd.read_csv(kwargs.get("dest") + "transformed/tournaments.csv")["OverviewPage"]
    obj_ = s3.get_key('transformed/tournaments.csv', 'leaguepedia')
    read_buffer_ = BytesIO()
    obj_.download_fileobj(read_buffer_)
    read_buffer_.seek(0)
    major_regions = pd.read_csv(read_buffer_)["OverviewPage"]
    filtered_tournamentresults = tournamentresults_pd[tournamentresults_pd["OverviewPage"].isin(major_regions)]
    filtered_TCL = filtered_tournamentresults[~filtered_tournamentresults["OverviewPage"].str.contains("TCL")]
    rename_teams = {"DWG KIA": "Dplus KIA", "KOO Tigers": "ROX Tigers"}
    filtered_TCL["Team"] = filtered_TCL["Team"].replace(rename_teams)
    write_buffer = BytesIO()
    filtered_TCL.to_csv(write_buffer, index=False, mode='wb', encoding='utf-8')
    write_buffer.seek(0)
    s3.load_file_obj(write_buffer, bucket_name='leaguepedia', key='transformed/tournamentresults.csv')
    return 

def scoreboardgames_transformer(**kwargs):
    # scoreboardgames_pd = pd.read_csv("../staging/extracted/scoreboardgames.csv")
    s3 = kwargs.get("s3")
    obj = s3.get_key('extracted/scoreboardgames.csv', 'leaguepedia')
    read_buffer = BytesIO()
    obj.download_fileobj(read_buffer)
    read_buffer.seek(0)
    scoreboardgames_pd = pd.read_csv(read_buffer)

    read_buffer_ = BytesIO()
    obj_ = s3.get_key('transformed/tournaments.csv', 'leaguepedia')
    obj_.download_fileobj(read_buffer_)
    read_buffer_.seek(0)
    major_regions = pd.read_csv(read_buffer_)["OverviewPage"]
    filtered_scoreboardgames = scoreboardgames_pd[scoreboardgames_pd["OverviewPage"].isin(major_regions)]
    rename_teams = {"DWG KIA": "Dplus KIA", "KOO Tigers": "ROX Tigers"}
    filtered_scoreboardgames["Team"] = filtered_scoreboardgames["Team"].replace(rename_teams)
    write_buffer = BytesIO()
    filtered_scoreboardgames.to_csv(write_buffer, index=False, mode='wb', encoding='utf-8')
    write_buffer.seek(0)
    s3.load_file_obj(write_buffer, bucket_name='leaguepedia', key='transformed/scoreboardgames.csv')
    return 

def teams_transformer(**kwargs):
    # teams_pd = pd.read_csv("../staging/extracted/teams.csv",dtype={"IsDisbanded":bool})
    s3 = kwargs.get("s3")
    obj = s3.get_key('extracted/teams.csv', 'leaguepedia')
    read_buffer = BytesIO()
    obj.download_fileobj(read_buffer)
    read_buffer.seek(0)
    teams_pd = pd.read_csv(read_buffer)
    teams_pd["IsDisbanded"] = teams_pd["IsDisbanded"].astype(bool)
    write_buffer = BytesIO()
    teams_pd.to_csv(write_buffer, index=False, mode='wb', encoding='utf-8')
    write_buffer.seek(0)
    s3.load_file_obj(write_buffer, bucket_name='leaguepedia', key='transformed/teams.csv')
    return 

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