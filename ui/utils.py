import polars as pl


def get_range(column):
    return [column.min(), column.max()]


def get_sorted_options(df, column, sort_col=None):
    return (
        df[[column, f"{column}_id" if sort_col is None else sort_col]]
        .unique()
        .sort(pl.col(f"{column}_id" if sort_col is None else sort_col))[column]
        .to_list()
    )
