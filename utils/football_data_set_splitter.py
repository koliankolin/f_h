import pandas as pd
import football_data_set as fds


class FootballDataSetSplitter:
    def __init__(self, train: bool = True):
        self.prefix = "train" if train else "test"
        self.data_set_path = "../data/train.csv" if train else "../data/test.csv"
        self.data_set: fds.FootballDataSet = fds.FootballDataSet(self.data_set_path)

    def get_player_data_set(
            self,
            stats_type: fds.StatsVariableType = None,
            var_type: fds.PlayerVariableType = None,
            add_common_cols: bool = False,
            as_pickle: bool = False,
            as_csv: bool = False,
            path: str = None
    ) -> pd.DataFrame:
        if as_csv and as_pickle:
            raise ValueError("Invalid save args")

        columns = (
            self.data_set.get_player_columns_by_var_type(var_type, add_common_cols)
            if var_type and not stats_type
            else self.data_set.get_stats_player_columns(stats_type, var_type, add_common_cols)
            if var_type and stats_type
            else self.data_set.get_player_columns(add_common_cols)
        )
        data = self.data_set.data[columns]

        if as_pickle and path:
            self.data_set.save_as_pickle(data=data, path=path)

        if as_csv and path:
            self.data_set.save_as_csv(data=data, path=path)

        return data

    def get_team_data_set(
            self,
            team_type: fds.TeamType = None,
            stats_type: fds.StatsVariableType = None,
            var_type: fds.TeamVariableType = None,
            add_common_cols: bool = False,
            as_pickle: bool = False,
            as_csv: bool = False,
            path: str = None
    ) -> pd.DataFrame:
        if as_csv and as_pickle:
            raise ValueError("Invalid save args")

        columns = (
            self.data_set.get_team_columns(team_type, add_common_cols)
            if team_type and not var_type and not stats_type
            else self.data_set.get_team_columns_by_var_type(var_type, team_type, add_common_cols)
            if var_type and var_type and not stats_type
            else self.data_set.get_stats_team_columns(team_type, stats_type, var_type, add_common_cols)
        )
        data = self.data_set.data[columns]

        if as_pickle and path:
            self.data_set.save_as_pickle(data=data, path=path)

        if as_csv and path:
            self.data_set.save_as_csv(data=data, path=path)

        return data


