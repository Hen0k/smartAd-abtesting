import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler, Normalizer


class CleanDataFrame:

    @staticmethod
    def get_numerical_columns(df: pd.DataFrame) -> list:
        numerical_columns = df.select_dtypes(include='number').columns.tolist()
        return numerical_columns

    @staticmethod
    def get_categorical_columns(df: pd.DataFrame) -> list:
        categorical_columns = df.select_dtypes(
            include=['object']).columns.tolist()
        return categorical_columns

    def isolate_relavant_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        relevant_columns = ['Bearer Id',
                            'Dur. ms',
                            'MSISDN/Number',
                            'Handset Manufacturer',
                            'Handset Type',
                            'Activity Duration DL (ms)',
                            'Activity Duration UL (ms)',
                            'Social Media DL (Bytes)',
                            'Social Media UL (Bytes)',
                            'Google DL (Bytes)',
                            'Google UL (Bytes)',
                            'Email DL (Bytes)',
                            'Email UL (Bytes)',
                            'Youtube DL (Bytes)',
                            'Youtube UL (Bytes)',
                            'Netflix DL (Bytes)',
                            'Netflix UL (Bytes)',
                            'Gaming DL (Bytes)',
                            'Gaming UL (Bytes)',
                            'Other DL (Bytes)',
                            'Other UL (Bytes)',
                            'Total UL (Bytes)',
                            'Total DL (Bytes)',
                            ]
        return df[relevant_columns]

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df.rename(columns={
            "Dur. (ms).1": "Dur. ms",
            "Dur. (ms)": "Dur. s"},
            inplace=True)

        return df

    def fix_datatypes(self, df: pd.DataFrame, column: str = None, to_type: type = None) -> pd.DataFrame:
        """
        Takes in the tellco dataframe an casts columns to proper data types.
        Start and End -> from string to datetime.
        Bearer Id, IMSI, MSISDN, IMEI -> From number to string
        """
        datetime_columns = ['Start',
                            'End', ]
        string_columns = [
            'IMSI',
            'MSISDN/Number',
            'IMEI',
            'Bearer Id'
        ]
        df_columns = df.columns
        for col in string_columns:
            if col in df_columns:
                df[col] = df[col].astype(str)
        for col in datetime_columns:
            if col in df_columns:
                df[col] = pd.to_datetime(df[col])
        if column and to_type:
            df[column] = df[column].astype(to_type)

        return df

    def percent_missing(self, df):
        """
        Print out the percentage of missing entries in a dataframe
        """
        # Calculate total number of cells in dataframe
        totalCells = np.product(df.shape)

        # Count number of missing values per column
        missingCount = df.isnull().sum()

        # Calculate total number of missing values
        totalMissing = missingCount.sum()

        # Calculate percentage of missing values
        print("The dataset contains", round(
            ((totalMissing/totalCells) * 100), 2), "%", "missing values.")

    def get_mct(self, series: pd.Series, measure: str):
        """
        get mean, median or mode depending on measure
        """
        measure = measure.lower()
        if measure == "mean":
            return series.mean()
        elif measure == "median":
            return series.median()
        elif measure == "mode":
            return series.mode()[0]

    def replace_missing(self, df: pd.DataFrame, columns: str, method: str) -> pd.DataFrame:

        for column in columns:
            nulls = df[column].isnull()
            indecies = [i for i, v in zip(nulls.index, nulls.values) if v]
            replace_with = self.get_mct(df[column], method)
            df.loc[indecies, column] = replace_with

        return df

    def remove_null_row(self, df: pd.DataFrame, columns: str) -> pd.DataFrame:
        for column in columns:
            df = df[~ df[column].isna()]

        return df

    def handle_missing_value(self, df: pd.DataFrame, verbose=True) -> pd.DataFrame:
        if verbose:
            self.percent_missing(df)
            print(df.isnull().sum())
        numericals = self.get_numerical_columns(df)
        objects = self.get_categorical_columns(df)
        for col in objects:
            df = df[df[col] != "nan"]
            # df[col].fillna(df[col].mode(), inplace=True)
            # df[col].dropna(inplace=True)
        for col in numericals:
            df[col].fillna(df[col].median(), inplace=True)
        # if numericals:
        # numeric_pipeline = Pipeline(steps=[
        #     ('impute', SimpleImputer(strategy='median')),
        #     # ('scale', MinMaxScaler()),
        #     # ('normalize', Normalizer()),
        # ])
        # cleaned_numerical = pd.DataFrame(
        #     numeric_pipeline.fit_transform(df[numericals]))
        # cleaned_numerical.columns = numericals

        # # if objects:
        # object_pipeline = Pipeline(steps=[
        #     ('impute', SimpleImputer(strategy='most_frequent'))
        # ])
        # cleaned_object = pd.DataFrame(
        #     object_pipeline.fit_transform(df[objects]))
        # cleaned_object.columns = objects

        # # if cleaned_object and cleaned_numerical:
        # cleaned_df = pd.concat([cleaned_object, cleaned_numerical], axis=1)
        if verbose:
            print(df.info())
            print(
                "="*10, "missing values imputed, collumns scalled, and normalized", "="*10)

        return df

    def normal_scale(self, df: pd.DataFrame) -> pd.DataFrame:
        scaller = StandardScaler()
        scalled = pd.DataFrame(scaller.fit_transform(
            df[self.get_numerical_columns(df)]))
        scalled.columns = self.get_numerical_columns(df)

        return scalled

    def minmax_scale(self, df: pd.DataFrame) -> pd.DataFrame:
        scaller = MinMaxScaler()
        scalled = pd.DataFrame(
            scaller.fit_transform(
                df[self.get_numerical_columns(df)]),
            columns=self.get_numerical_columns(df)
        )

        return scalled

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        normalizer = Normalizer()
        normalized = pd.DataFrame(
            normalizer.fit_transform(
                df[self.get_numerical_columns(df)]),
            columns=self.get_numerical_columns(df)
        )

        return normalized
