import argparse
import os
from datetime import datetime, timezone

import dash
import dash_bootstrap_components as dbc
import psycopg2
import psycopg2.extras
from dash import Input, Output, callback, dcc, html

STATUS_COLORS: dict[str, str] = {
    "done": "#34c759",
    "corrupted": "#ff3b30",
    "pending": "#d8d8d8",
    "processing": "#007aff",
}

CARD_BG = "#ffffff"
PAGE_BG = "#f5f5f7"
SURFACE = "#ffffff"
BORDER = "#e0e0e5"
TEXT_PRIMARY = "#1d1d1f"
TEXT_MUTED = "#86868b"
ACCENT = "#007aff"

DEFAULT_INTERVAL_S = 30


# ── DB helpers ─────────────────────────────────────────────────────────────────
def get_connection() -> psycopg2.extensions.connection:
    """Open a new psycopg2 connection."""
    return psycopg2.connect(
        host=os.environ["DISTRIBUTE_DB_HOST"],
        dbname=os.environ["DISTRIBUTE_DB_NAME"],
        user=os.environ["DISTRIBUTE_DB_USER"],
        password=os.environ["DISTRIBUTE_DB_PASSWORD"],
        port=int(os.environ.get("DISTRIBUTE_DB_PORT", "5432")),
    )


def fetch_jobs() -> list[dict]:
    """Fetch all job tables with their status counts and latest worker heartbeat."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                  AND table_name NOT LIKE '%_workers'
                ORDER BY table_name
                """
            )
            job_tables: list[str] = [row["table_name"] for row in cur.fetchall()]

            jobs: list[dict] = []
            for table in job_tables:
                cur.execute(
                    f"""
                    SELECT status, COUNT(*) AS cnt
                    FROM {psycopg2.extensions.quote_ident(table, cur)}
                    GROUP BY status
                    """
                )
                counts: dict[str, int] = {
                    r["status"]: int(r["cnt"]) for r in cur.fetchall()
                }

                workers_table = f"{table}_workers"
                latest_heartbeat: datetime | None = None
                num_workers: int = 0
                try:
                    cur.execute(
                        f"""
                        SELECT MAX(heartbeat) AS latest, COUNT(*) AS cnt
                        FROM {psycopg2.extensions.quote_ident(workers_table, cur)}
                        """
                    )
                    row = cur.fetchone()
                    if row and row["latest"]:
                        latest_heartbeat = row["latest"]
                        num_workers = int(row["cnt"])
                except psycopg2.errors.UndefinedTable:
                    conn.rollback()

                jobs.append(
                    {
                        "name": table,
                        "counts": counts,
                        "total": sum(counts.values()),
                        "latest_heartbeat": latest_heartbeat,
                        "num_workers": num_workers,
                    }
                )

            jobs.sort(
                key=lambda j: j["latest_heartbeat"]
                or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
            return jobs
    finally:
        conn.close()


# ── UI helpers ─────────────────────────────────────────────────────────────────
def format_heartbeat(ts: datetime | None) -> str:
    """Format a heartbeat timestamp as a human-readable relative string."""
    if ts is None:
        return "no heartbeat"
    now = datetime.now(tz=timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = now - ts
    s = int(delta.total_seconds())
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s // 60}m ago"
    if s < 86400:
        return f"{s // 3600}h ago"
    return f"{s // 86400}d ago"


def make_progress_bar(counts: dict[str, int], total: int) -> html.Div:
    """Render a segmented multi-color progress bar."""
    if total == 0:
        return html.Div(
            style={
                "height": "10px",
                "borderRadius": "6px",
                "background": BORDER,
            }
        )

    order = ["done", "processing", "pending", "corrupted"]
    segments: list[html.Div] = []
    for status in order:
        count = counts.get(status, 0)
        if count == 0:
            continue
        pct = count / total * 100
        segments.append(
            html.Div(
                style={
                    "width": f"{pct}%",
                    "height": "10px",
                    "background": STATUS_COLORS[status],
                    "borderRadius": "0",
                }
            )
        )

    if segments:
        segments[0].style["borderRadius"] = "6px 0 0 6px"  # type: ignore[index]
        segments[-1].style["borderRadius"] = (  # type: ignore[index]
            "6px" if len(segments) == 1 else "0 6px 6px 0"
        )

    return html.Div(
        segments,
        style={
            "display": "flex",
            "borderRadius": "6px",
            "overflow": "hidden",
            "background": BORDER,
        },
    )


def make_stat_pill(label: str, value: int, color: str) -> html.Span:
    """Render a small colored count pill."""
    return html.Span(
        [
            html.Span(str(value), style={"fontWeight": "700", "color": color}),
            html.Span(f" {label}", style={"color": TEXT_MUTED}),
        ],
        style={
            "fontSize": "12px",
            "padding": "2px 8px",
            "borderRadius": "999px",
            "background": f"{color}18",
            "border": f"1px solid {color}40",
        },
    )


def make_job_card(job: dict) -> html.Div:
    """Render a single job card."""
    counts = job["counts"]
    total = job["total"]
    heartbeat_str = format_heartbeat(job["latest_heartbeat"])

    pills = [
        make_stat_pill(status, counts.get(status, 0), STATUS_COLORS[status])
        for status in ["done", "processing", "pending", "corrupted"]
        if counts.get(status, 0) > 0
    ]

    done = counts.get("done", 0) + counts.get("corrupted", 0)
    pct_done = int(done / total * 100) if total else 0

    return html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.Span(
                                job["name"],
                                style={
                                    "fontFamily": "ui-monospace, 'SF Mono', 'Menlo', monospace",
                                    "fontWeight": "500",
                                    "fontSize": "14px",
                                    "color": TEXT_PRIMARY,
                                    "letterSpacing": "0",
                                },
                            ),
                        ]
                    ),
                    html.Span(
                        heartbeat_str,
                        style={
                            "fontSize": "14px",
                            "color": TEXT_PRIMARY,
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "marginBottom": "10px",
                },
            ),
            make_progress_bar(counts, total),
            html.Div(
                [
                    html.Div(
                        pills,
                        style={"display": "flex", "gap": "6px", "flexWrap": "wrap"},
                    ),
                    html.Span(
                        f"{pct_done}% complete · {job['num_workers']} worker{'s' if job['num_workers'] != 1 else ''} · {total:,} tasks",
                        style={
                            "fontSize": "12px",
                            "color": TEXT_MUTED,
                        },
                    ),
                ],
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "marginTop": "10px",
                    "flexWrap": "wrap",
                    "gap": "6px",
                },
            ),
        ],
        style={
            "background": CARD_BG,
            "border": f"1px solid {BORDER}",
            "borderRadius": "12px",
            "padding": "16px 20px",
            "marginBottom": "8px",
            "boxShadow": "0 1px 3px rgba(0,0,0,0.06)",
        },
    )


def create_app() -> dash.Dash:
    title = f"Task Monitor: {os.environ['DISTRIBUTE_DB_NAME']}"
    app = dash.Dash(
        __name__,
        external_stylesheets=[
            "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap",
            dbc.themes.BOOTSTRAP,
        ],
        title=title,
    )

    app.layout = html.Div(
        [
            # Sticky top chrome (header + search)
            html.Div(
                [
                    # Header
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H1(
                                        title,
                                        style={
                                            "fontFamily": "-apple-system, BlinkMacSystemFont, 'Inter', sans-serif",
                                            "fontWeight": "600",
                                            "fontSize": "18px",
                                            "color": TEXT_PRIMARY,
                                            "margin": "0",
                                        },
                                    ),
                                    html.Div(
                                        id="last-updated",
                                        style={"fontSize": "12px", "color": TEXT_MUTED},
                                    ),
                                ]
                            ),
                            html.Div(
                                [
                                    html.Button(
                                        "↻ Refresh",
                                        id="refresh-btn",
                                        n_clicks=0,
                                        style={
                                            "background": "#ffffff",
                                            "border": f"1px solid {BORDER}",
                                            "color": TEXT_PRIMARY,
                                            "borderRadius": "8px",
                                            "padding": "6px 14px",
                                            "cursor": "pointer",
                                            "fontSize": "13px",
                                            "fontFamily": "-apple-system, BlinkMacSystemFont, 'Inter', sans-serif",
                                            "fontWeight": "500",
                                            "boxShadow": "0 1px 2px rgba(0,0,0,0.06)",
                                        },
                                    ),
                                ],
                                style={
                                    "display": "flex",
                                    "alignItems": "center",
                                    "gap": "16px",
                                },
                            ),
                        ],
                        style={
                            "display": "flex",
                            "justifyContent": "space-between",
                            "alignItems": "center",
                            "padding": "18px 28px 16px",
                            "borderBottom": f"1px solid {BORDER}",
                            "background": "#ffffff",
                            "flexWrap": "wrap",
                            "gap": "12px",
                        },
                    ),
                    # Search bar
                    html.Div(
                        dcc.Input(
                            id="search-input",
                            type="text",
                            placeholder="Search jobs…",
                            debounce=False,
                            style={
                                "width": "100%",
                                "background": "#ffffff",
                                "border": f"1px solid {BORDER}",
                                "borderRadius": "8px",
                                "color": TEXT_PRIMARY,
                                "padding": "9px 14px",
                                "fontSize": "14px",
                                "fontFamily": "-apple-system, BlinkMacSystemFont, 'Inter', sans-serif",
                                "outline": "none",
                                "boxSizing": "border-box",
                                "boxShadow": "0 1px 2px rgba(0,0,0,0.04)",
                            },
                        ),
                        style={"padding": "14px 28px"},
                    ),
                    # Divider
                    html.Div(
                        style={
                            "margin": "0",
                            "borderTop": f"1px solid {BORDER}",
                        }
                    ),
                ],
                style={
                    "position": "sticky",
                    "top": "0",
                    "zIndex": "100",
                    "background": PAGE_BG,
                },
            ),
            # Scrollable job list
            html.Div(
                id="job-list",
                style={
                    "padding": "16px 28px 28px",
                    "overflowY": "auto",
                    "flex": "1",
                },
            ),
        ],
        style={
            "background": PAGE_BG,
            "height": "100vh",
            "display": "flex",
            "flexDirection": "column",
            "fontFamily": "-apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', sans-serif",
            "overflow": "hidden",
        },
    )

    # Job List Update
    @callback(
        Output("job-list", "children"),
        Output("last-updated", "children"),
        Input("refresh-btn", "n_clicks"),
        Input("search-input", "value"),
    )
    def update_jobs(
        _n_clicks: int,
        search: str | None,
    ) -> tuple[list, str]:
        """Fetch jobs from the database and render job cards."""
        try:
            jobs = fetch_jobs()
        except Exception as exc:
            return (
                [
                    html.Div(
                        f"Error connecting to database: {exc}",
                        style={"color": "#ef4444", "padding": "20px"},
                    )
                ],
                "failed to fetch",
            )

        query = (search or "").strip().lower()
        if query:
            jobs = [j for j in jobs if query in j["name"].lower()]

        now_str = datetime.now().strftime("Updated %H:%M:%S")

        if not jobs:
            return (
                [
                    html.Div(
                        "No jobs found.",
                        style={
                            "color": TEXT_MUTED,
                            "padding": "40px 0",
                            "textAlign": "center",
                            "fontSize": "13px",
                        },
                    )
                ],
                now_str,
            )

        return [make_job_card(j) for j in jobs], now_str

    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    app = create_app()
    app.run(host="0.0.0.0", port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
