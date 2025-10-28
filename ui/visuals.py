import plotly.express as px
import streamlit as st


def render_stats_table(df):
    return st.dataframe(df,
                column_config={
                    "player_team": st.column_config.TextColumn("Player"),
                    "position": st.column_config.TextColumn("Pos"),
                    "cost": st.column_config.NumberColumn("Cost", format="%.1f"),
                    "form": st.column_config.NumberColumn("Form", format="%.1f"),
                    "form_change_last_3": st.column_config.NumberColumn("Δ3", format="%.1f"),
                    "dreamteam": st.column_config.CheckboxColumn("Curr. Dream."),
                    "dreamteam_appearances": st.column_config.NumberColumn("Dream. Apps", format="%.0f"),
                    "availability": st.column_config.NumberColumn("Av. %", format="%.0f"),
                    "appearances_total": st.column_config.NumberColumn("Apps", format="%.0f"),
                    "minutes_total": st.column_config.NumberColumn("Mins", format="%.0f"),
                    "minutes_last_3": st.column_config.NumberColumn("(3)", format="%.0f"),
                    "minutes_gameweek": st.column_config.NumberColumn("/GW", format="%.0f"),
                    "points_total": st.column_config.NumberColumn("Pts", format="%.0f"),
                    "points_last_3": st.column_config.NumberColumn("(3)", format="%.0f"),
                    "points_gameweek": st.column_config.NumberColumn("/GW", format="%.1f"),
                    "points_90": st.column_config.NumberColumn("/90", format="%.1f"),
                    "points_cost": st.column_config.NumberColumn("/£M", format="%.1f"),
                    "goals_total": st.column_config.NumberColumn("G", format="%.0f"),
                    "xgoals_total": st.column_config.NumberColumn("xG", format="%.2f"),
                    "g_xg_total": st.column_config.NumberColumn("G/xG", format="%.2f"),
                    "fixtures_next_3": st.column_config.TextColumn("Fixtures"),
                    "prospects_icon_next_3": st.column_config.TextColumn("Prospects"),
                    "defending_prospects_next_3": st.column_config.NumberColumn("Def. Prs", format="%.2f"),
                    "attacking_prospects_next_3": st.column_config.NumberColumn("Att. Prs", format="%.2f"),
                    })


def goal_xg_plot(df):
    fig = px.scatter(
        df,
        x='xgoals_total',
        y='g_xg_total',
        color='position',
        custom_data=['player_team', 'goals_total'],  # Use custom_data parameter instead
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
        x=df['xgoals_total'].max() * 0.95,
        y=1.15,
        text="Good",
        showarrow=False,
        font=dict(size=12, color="green")
    )
    
    fig.add_annotation(
        x=df['xgoals_total'].max() * 0.95,
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

    return fig