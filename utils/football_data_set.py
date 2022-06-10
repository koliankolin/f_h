from enum import Enum
import pickle
import pandas as pd


class StatsVariableType(Enum):
    RAW = 'raw'
    DERIVED = 'derived'
    RATIO = 'ratio'


class PlayerVariableType(Enum):
    OFFENSIVE = 'offensive'
    DEFENSIVE = 'defensive'
    POSITIONAL = 'positional'
    PHYSICAL = 'physical'
    GENERAL = 'general'
    OTHER = 'other'


class TeamType(Enum):
    FIRST = '1'
    SECOND = '2'


class TeamVariableType(Enum):
    OFFENSIVE = 'offensive'
    DEFENSIVE = 'defensive'
    OTHER = 'other'


class FootballDataSet:
    COMMON_COLUMNS = ['row_id', 'scout_id']

    def __init__(self, csv_path: str):
        self.data = pd.read_csv(csv_path)

    @staticmethod
    def save_as_pickle(data: pd.DataFrame, path: str):
        with open(path) as f:
            pickle.dump(data, f)

    @staticmethod
    def save_as_csv(data: pd.DataFrame, path: str):
        data.to_csv(path)

    @staticmethod
    def read_from_pickle(pkl_path) -> pd.DataFrame:
        with open(pkl_path) as f:
            data = pickle.load(f)

        return data

    def get_player_columns(self, add_common_cols: bool = False) -> list:
        columns = self._get_columns_by_regex("player.*")
        return self._add_common_cols(columns) if add_common_cols else columns

    def get_player_columns_by_var_type(self, var_type: PlayerVariableType, add_common_cols: bool = False):
        columns = self._get_columns_by_regex(f"player_{var_type}_.*")
        return self._add_common_cols(columns) if add_common_cols else columns

    def get_team_columns(self, team_type: TeamType = TeamType.FIRST, add_common_cols: bool = False) -> list:
        columns = self._get_columns_by_regex(f"team{team_type}.*")
        return self._add_common_cols(columns) if add_common_cols else columns

    def get_team_columns_by_var_type(
            self,
            var_type: TeamVariableType,
            team_type: TeamType = TeamType.FIRST,
            add_common_cols: bool = False
    ) -> list:
        columns = self._get_columns_by_regex(f"team{team_type}_{var_type}.*")
        return self._add_common_cols(columns) if add_common_cols else columns

    def get_stats_player_columns(
            self,
            stats_type: StatsVariableType,
            var_type: PlayerVariableType = None,
            add_common_cols: bool = False
    ) -> list:
        columns = (
            self.get_player_columns(add_common_cols)
            if not var_type
            else self.get_player_columns_by_var_type(var_type, add_common_cols)
        )
        filtered_columns = self._get_filtered_columns_by_stats_type(columns, stats_type)

        return self._add_common_cols(filtered_columns) if add_common_cols else filtered_columns

    def get_stats_team_columns(
            self,
            team_type: TeamType,
            stats_type: StatsVariableType,
            var_type: TeamVariableType = None,
            add_common_cols: bool = False
    ) -> list:
        columns = (
            self.get_team_columns(team_type, add_common_cols)
            if not var_type
            else self.get_team_columns_by_var_type(var_type, team_type, add_common_cols)
        )
        filtered_columns = self._get_filtered_columns_by_stats_type(columns, stats_type)

        return self._add_common_cols(filtered_columns) if add_common_cols else filtered_columns

    def _get_columns_by_regex(self, regex: str) -> list:
        return list(self.data.head(1).filter(regex=regex).columns)

    @staticmethod
    def _add_common_cols(columns: list) -> list:
        return FootballDataSet.COMMON_COLUMNS + columns

    @staticmethod
    def _get_filtered_columns_by_stats_type(columns: list, stats_type: StatsVariableType) -> list:
        return list(filter(lambda x: stats_type in x, columns))
