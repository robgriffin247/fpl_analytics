import streamlit as st
import duckdb
import polars as pl
from loaders import load_obt_players_df, load_gameweek_players_df
import plotly.express as px

# FRONT END =================================================================================
st.set_page_config(layout="wide", page_title="FPL Analytics")

st.header("Analytics")
t1, t2, t3 = st.tabs(["Stats", "Compare", "Fixtures & Standings"])

with t1:
    # Filters
    t1_c1, t1_c2, t1_c3, t1_c4 = st.columns([5,3,3,3], gap="large")
    st.divider()
    # Table
    stats_table = st.container()
    # Column selector


with t2:
    player_selector = st.container()

with t3:
    standings_table = st.container()

# BACK END ==================================================================================
# General
obt_players_df = load_obt_players_df()
gameweek_players_df = load_gameweek_players_df(obt_players_df)

def get_range(column):
    return [gameweek_players_df[column].min(), gameweek_players_df[column].max()]

def get_options(column, sort=None):
    if sort!=None:
        return gameweek_players_df[[column, sort]].unique().sort(pl.col(sort))[column].to_list()
    
    return gameweek_players_df[[column]].unique().sort(pl.col(column))[column].to_list()

position_options = get_options("position", "position_id")
player_options = get_options("player_team", "player_id")
team_options = get_options("team")

cost_range = get_range("cost")
form_range = get_range("form")
availability_range = get_range("availability")

appearances_range = get_range("appearances_total")
starts_range = get_range("starts_total")
minutes_gameweek_range = get_range("minutes_gameweek")

points_gameweek_range = get_range("points_gameweek")
defending_prospects_range = get_range("defending_prospects_next_3")
attacking_prospects_range = get_range("attacking_prospects_next_3")


# T1
selected_positions = t1_c1.multiselect("Positon(s)", options=position_options)
selected_players_t1 = t1_c1.multiselect("Player(s)", options=player_options, key="selected_players_t1")
selected_teams = t1_c1.multiselect("Team(s)", options=team_options)

selected_cost = t1_c2.slider("Cost (£M)", value=cost_range, min_value=cost_range[0], max_value=cost_range[1], step=0.1)
selected_form = t1_c2.slider("Form", value=form_range, min_value=form_range[0], max_value=form_range[1], step=0.1)
selected_availability = t1_c2.slider("Availability", value=availability_range, min_value=availability_range[0], max_value=availability_range[1], step=25)

selected_appearances = t1_c3.slider("Appearances", value=appearances_range, min_value=appearances_range[0], max_value=appearances_range[1], step=1)
selected_starts = t1_c3.slider("Starts", value=starts_range, min_value=starts_range[0], max_value=starts_range[1], step=1)
selected_minutes_gameweek = t1_c3.slider("Mins/GW", value=minutes_gameweek_range, min_value=minutes_gameweek_range[0], max_value=minutes_gameweek_range[1], step=1.0)

selected_defending_prospects = t1_c4.slider("Def. Prospects", value=defending_prospects_range, min_value=defending_prospects_range[0], max_value=defending_prospects_range[1], step=1.0)
selected_attacking_prospects = t1_c4.slider("Att. Prospects", value=attacking_prospects_range, min_value=attacking_prospects_range[0], max_value=attacking_prospects_range[1], step=1.0)
selected_points_gameweek = t1_c4.slider("Pts/GW", value=points_gameweek_range, min_value=points_gameweek_range[0], max_value=points_gameweek_range[1], step=1.0)



with duckdb.connect() as con:
    filtered_gameweek_players_df = con.sql(
        f"""
            select 
                player_team, 
                position,
                cost,
                availability,
                form,
                dreamteam,
                dreamteam_appearances,
                minutes_total,
                minutes_gameweek,
                points_total,
                points_gameweek,
                points_90,
                points_cost,
                goals_total,
                g_xg_total,
                fixtures_next_3,
                prospects_icon_next_3,
                defending_prospects_next_3,
                attacking_prospects_next_3,

                minutes_played_available_last_3,
                points_last_3,
                minutes_last_3,
                goals_last_3,
                xgoals_last_3,
                g_xg_last_3,
                form_change_last_3,

            from gameweek_players_df 
            where true 
                and position in {selected_positions if len(selected_positions)>0 else position_options}
                and player_team in {selected_players_t1 if len(selected_players_t1)>0 else player_options}
                and team in {selected_teams if len(selected_teams)>0 else team_options}
                and cost between {selected_cost[0]} and {selected_cost[1]}
                and form between {selected_form[0]} and {selected_form[1]}
                and availability between {selected_availability[0]} and {selected_availability[1]}
                and appearances_total between {selected_appearances[0]} and {selected_appearances[1]}
                and starts_total between {selected_starts[0]} and {selected_starts[1]}
                and minutes_gameweek between {selected_minutes_gameweek[0]} and {selected_minutes_gameweek[1]}
                and defending_prospects_next_3 between {selected_defending_prospects[0]} and {selected_defending_prospects[1]}
                and attacking_prospects_next_3 between {attacking_prospects_range[0]} and {attacking_prospects_range[1]}
                and points_gameweek between {selected_points_gameweek[0]} and {selected_points_gameweek[1]}

        """).pl()


def render_stats_table():
    return st.dataframe(filtered_gameweek_players_df,
                 column_config={
                     "player_team": st.column_config.TextColumn("Player"),
                     "position": st.column_config.TextColumn("Pos"),
                     "cost": st.column_config.NumberColumn("Cost", format="%.1f"),
                     "form": st.column_config.NumberColumn("Form", format="%.1f"),
                     "dreamteam": st.column_config.CheckboxColumn("Curr. Dream."),
                     "dreamteam_appearances": st.column_config.NumberColumn("Dream. Apps", format="%.0f"),
                     "availability": st.column_config.NumberColumn("Av. %", format="%.0f"),
                     "appearances_total": st.column_config.NumberColumn("Apps", format="%.0f"),
                     "minutes_total": st.column_config.NumberColumn("Mins", format="%.0f"),
                     "minutes_gameweek": st.column_config.NumberColumn("/GW", format="%.0f"),
                     "points_total": st.column_config.NumberColumn("Pts", format="%.0f"),
                     "points_gameweek": st.column_config.NumberColumn("/GW", format="%.1f"),
                     "points_90": st.column_config.NumberColumn("/90", format="%.1f"),
                     "points_cost": st.column_config.NumberColumn("/£M", format="%.1f"),
                     "goals_total": st.column_config.NumberColumn("Gls", format="%.0f"),
                     "g_xg_total": st.column_config.NumberColumn("G/xG", format="%.2f"),
                     "fixtures_next_3": st.column_config.TextColumn("Fixtures"),
                     "prospects_icon_next_3": st.column_config.TextColumn("Prospects"),
                     "defending_prospects_next_3": st.column_config.NumberColumn("Def. Prs", format="%.2f"),
                     "attacking_prospects_next_3": st.column_config.NumberColumn("Att. Prs", format="%.2f"),
                     })

render_stats_table()

# T2
selected_players_t2 = player_selector.multiselect("Player(s)", options=player_options, max_selections=5, key="selected_players_t2")

with duckdb.connect() as con:
    filtered_players_df = (con.sql(
        f"""select * 
            from obt_players_df 
            where player_team in {selected_players_t2}
            """).pl())

with t2:
    if filtered_players_df.shape[0] > 0:
        st.dataframe(filtered_players_df)

    


# T3
with t3:
# Filter the data
    filtered_df = obt_players_df.filter(pl.col('gameweek') == 8).filter(pl.col("xgoals_total")>0)

    fig = px.scatter(
        filtered_df,
        x='xgoals_total',
        y='g_xg_total',
        color='position',
        custom_data=['player', 'goals_total'],  # Use custom_data parameter instead
        title='xG and Conversion Rate',
        labels={
            'xgoals_total': 'Expected Goals (Season)',
            'g_xg_total': 'Conversion Rate (G/xG)',
            "position": "Position"
        }
    )
    
    # Add horizontal line at y=1
    fig.add_hline(y=1, line_dash="dash", line_color="gray", opacity=0.7)
    
    # Add "Good" and "Bad" annotations
    fig.add_annotation(
        x=filtered_df['xgoals_total'].max() * 0.95,
        y=1.15,
        text="Good",
        showarrow=False,
        font=dict(size=12, color="green")
    )
    
    fig.add_annotation(
        x=filtered_df['xgoals_total'].max() * 0.95,
        y=0.85,
        text="Bad",
        showarrow=False,
        font=dict(size=12, color="red")
    )
    
    # Customize hover template
    fig.update_traces(
        hovertemplate='<b>%{customdata[0]}</b><br>G: %{customdata[1]}<br>xG: %{x:.2f}<br>G/xG: %{y:.2f}<extra></extra>',
        selector=dict(mode='markers')  # Only update the scatter points, not trendlines
    )

    st.plotly_chart(fig)