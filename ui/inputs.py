import streamlit as st
from utils import get_range


def create_slider(df, column, title, hint=None, step=None):

    _range = get_range(df[column])

    if step is not None:
        return st.slider(
            title,
            value=_range,
            min_value=_range[0],
            max_value=_range[1],
            key=f"selected_{column}",
            help=hint,
            step=step,
        )

    return st.slider(
        title,
        value=_range,
        min_value=_range[0],
        max_value=_range[1],
        key=f"selected_{column}",
        help=hint,
    )
