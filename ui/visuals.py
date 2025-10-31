import plotly.express as px
import streamlit as st


def render_filtered_current_player_stats(df):
    out = st.dataframe(
        df,
        column_config={
            "player": st.column_config.TextColumn("Player"),
            "team": st.column_config.TextColumn("Team"),
            "position": st.column_config.TextColumn("Position"),
            "cost": st.column_config.NumberColumn("Cost", format="%.1f"),
            "minutes_gw": st.column_config.NumberColumn("Mins/wk", format="%.0f"),
            "minutes_last_3": st.column_config.NumberColumn("Mins (3)", format="%.0f"),
            "points_gw": st.column_config.NumberColumn("Pts/wk", format="%.1f"),
            "points_last_3": st.column_config.NumberColumn("Pts (3)", format="%.1f"),
            "points_90": st.column_config.NumberColumn("Pts/90", format="%.1f"),
            "points_90": st.column_config.NumberColumn("Pts/90", format="%.1f"),
            "points_cost_gw": st.column_config.NumberColumn("Pts/£M/wk", format="%.1f"),
            "goals_total": st.column_config.NumberColumn("Goals", format="%.0f"),
            "goals_gw": st.column_config.NumberColumn("Goals/wk", format="%.2f"),
            "xgoals_total": st.column_config.NumberColumn("xGoals", format="%.2f"),
            "xgoals_gw": st.column_config.NumberColumn("xGoals/wk", format="%.2f"),
            "goals_total_xratio": st.column_config.NumberColumn(
                "xG Conv.", format="%.2f"
            ),
            "availability_next": st.column_config.NumberColumn(
                "Av. (%)", format="%.0f"
            ),
            "xpoints_next": st.column_config.NumberColumn("xPts", format="%.1f"),
            "next_fixtures": st.column_config.TextColumn("Fixtures"),
            "form_icons": st.column_config.TextColumn("Team Form (TF)"),
            "next_fixture_form_icons": st.column_config.TextColumn("Opp. Form (OF)"),
            "form_points": st.column_config.NumberColumn("TF (Pts)", format="%.1f"),
            "next_fixture_form_points": st.column_config.NumberColumn(
                "OF (Pts)", format="%.1f"
            ),
            "form_scored": st.column_config.NumberColumn("TF (GF)", format="%.1f"),
            "next_fixture_form_scored": st.column_config.NumberColumn(
                "OF (GF)", format="%.1f"
            ),
            "form_conceded": st.column_config.NumberColumn("TF (GA)", format="%.1f"),
            "next_fixture_form_conceded": st.column_config.NumberColumn(
                "OF (GA)", format="%.1f"
            ),
        },
    )
    return out


def goal_xg_plot(df):
    fig = px.scatter(
        df,
        x="xgoals_total",
        y="g_xg_total",
        color="position",
        custom_data=["player_team", "goals_total"],  # Use custom_data parameter instead
        title="xG and Conversion Rate",
        labels={
            "xgoals_total": "Expected Goals (Season)",
            "g_xg_total": "Conversion Rate (G/xG)",
            "position": "Position",
        },
    )

    # Add horizontal line at y=1
    fig.add_hline(y=1, line_dash="dash", line_color="gray", opacity=0.7)

    # Add "Good" and "Bad" annotations
    fig.add_annotation(
        x=df["xgoals_total"].max() * 0.95,
        y=1.15,
        text="Good",
        showarrow=False,
        font=dict(size=12, color="green"),
    )

    fig.add_annotation(
        x=df["xgoals_total"].max() * 0.95,
        y=0.85,
        text="Bad",
        showarrow=False,
        font=dict(size=12, color="red"),
    )

    # Customize hover template
    fig.update_traces(
        hovertemplate="<b>%{customdata[0]}</b><br>G: %{customdata[1]}<br>xG: %{x:.2f}<br>G/xG: %{y:.2f}<extra></extra>",
        selector=dict(mode="markers"),  # Only update the scatter points, not trendlines
    )

    return fig
