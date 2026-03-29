"""Distributed task monitoring dashboard."""

import argparse
import os
from datetime import datetime, timezone

import dash
import dash_bootstrap_components as dbc
import psycopg2
import psycopg2.extras
from dash import Input, Output, callback, dcc, html

# ── Color palette ──────────────────────────────────────────────────────────────
STATUS_COLORS: dict[str, str] = {
    "success": "#34c759",
    "corrupt": "#ff3b30",
    "pending": "#ff9500",
    "processing": "#007aff",
}

CARD_BG = "#ffffff"
PAGE_BG = "#f5f5f7"
SURFACE = "#ffffff"
BORDER = "#e0e0e5"
TEXT_PRIMARY = "#1d1d1f"
TEXT_MUTED = "#86868b"
ACCENT = "#007aff"


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
            # Discover job tables (exclude *_workers tables)
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
                # Status counts
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

                # Latest heartbeat from workers table (may not exist)
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

            # Sort by most recent heartbeat (None goes last)
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

    order = ["success", "processing", "pending", "corrupt"]
    segments: list[html.Div] = []
    for i, status in enumerate(order):
        count = counts.get(status, 0)
        if count == 0:
            continue
        pct = count / total * 100
        radius = {}
        # First and last visible segments get rounded ends
        is_first = not segments
        segments.append(
            html.Div(
                style={
                    "width": f"{pct}%",
                    "height": "10px",
                    "background": STATUS_COLORS[status],
                    "borderRadius": "0",
                    **radius,
                    "transition": "width 0.4s ease",
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


def make_legend() -> html.Div:
    """Render the status color legend."""
    items = [
        html.Div(
            [
                html.Span(
                    style={
                        "display": "inline-block",
                        "width": "10px",
                        "height": "10px",
                        "borderRadius": "2px",
                        "background": color,
                        "marginRight": "5px",
                        "flexShrink": "0",
                    }
                ),
                html.Span(label, style={"fontSize": "11px", "color": TEXT_MUTED}),
            ],
            style={"display": "flex", "alignItems": "center", "gap": "2px"},
        )
        for label, color in STATUS_COLORS.items()
    ]
    return html.Div(items, style={"display": "flex", "gap": "16px", "flexWrap": "wrap"})


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
        for status in ["success", "processing", "pending", "corrupt"]
        if counts.get(status, 0) > 0
    ]

    done = counts.get("success", 0) + counts.get("corrupt", 0)
    pct_done = int(done / total * 100) if total else 0

    return html.Div(
        [
            # Header row
            html.Div(
                [
                    html.Div(
                        [
                            html.Span(
                                job["name"],
                                style={
                                    "fontFamily": "ui-monospace, 'SF Mono', 'Menlo', monospace",
                                    "fontWeight": "500",
                                    "fontSize": "13px",
                                    "color": TEXT_PRIMARY,
                                    "letterSpacing": "0",
                                },
                            ),
                        ]
                    ),
                    html.Div(
                        [
                            html.Span(
                                f"{job['num_workers']} worker{'s' if job['num_workers'] != 1 else ''}",
                                style={
                                    "fontSize": "12px",
                                    "color": TEXT_MUTED,
                                    "marginRight": "12px",
                                },
                            ),
                            html.Span(
                                f"♥ {heartbeat_str}",
                                style={
                                    "fontSize": "12px",
                                    "color": ACCENT,
                                    "fontFamily": "ui-monospace, 'SF Mono', monospace",
                                },
                            ),
                        ],
                        style={"display": "flex", "alignItems": "center"},
                    ),
                ],
                style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "marginBottom": "10px",
                },
            ),
            # Progress bar
            make_progress_bar(counts, total),
            # Footer row
            html.Div(
                [
                    html.Div(
                        pills,
                        style={"display": "flex", "gap": "6px", "flexWrap": "wrap"},
                    ),
                    html.Span(
                        f"{pct_done}% complete · {total:,} tasks",
                        style={
                            "fontSize": "11px",
                            "color": TEXT_MUTED,
                            "fontFamily": "ui-monospace, 'SF Mono', monospace",
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


# ── App factory ────────────────────────────────────────────────────────────────
def create_app(poll_interval_s: int = 30) -> dash.Dash:
    """Create and configure the Dash app."""
    app = dash.Dash(
        __name__,
        external_stylesheets=[
            "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap",
            dbc.themes.BOOTSTRAP,
        ],
        title="Task Monitor",
    )

    app.layout = html.Div(
        [
            dcc.Interval(
                id="auto-refresh",
                interval=poll_interval_s * 1000,
                n_intervals=0,
            ),
            # ── Page header ────────────────────────────────────────────────
            html.Div(
                [
                    html.Div(
                        [
                            html.H1(
                                "Task Monitor",
                                style={
                                    "fontFamily": "-apple-system, BlinkMacSystemFont, 'Inter', sans-serif",
                                    "fontWeight": "600",
                                    "fontSize": "18px",
                                    "color": TEXT_PRIMARY,
                                    "letterSpacing": "-0.01em",
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
                            make_legend(),
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
                                    "transition": "background 0.15s",
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
            # ── Search bar ─────────────────────────────────────────────────
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
                style={"padding": "16px 28px 8px"},
            ),
            # ── Job list ───────────────────────────────────────────────────
            html.Div(id="job-list", style={"padding": "8px 28px 28px"}),
        ],
        style={
            "background": PAGE_BG,
            "minHeight": "100vh",
            "fontFamily": "-apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', sans-serif",
        },
    )

    @callback(
        Output("job-list", "children"),
        Output("last-updated", "children"),
        Input("auto-refresh", "n_intervals"),
        Input("refresh-btn", "n_clicks"),
        Input("search-input", "value"),
    )
    def update_jobs(
        _n_intervals: int,
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


# ── Entry point ────────────────────────────────────────────────────────────────
def main() -> None:
    """Parse CLI args and launch the dashboard."""
    parser = argparse.ArgumentParser(
        description="Distributed task monitoring dashboard"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=30,
        help="Auto-refresh interval in seconds (default: 30)",
    )
    parser.add_argument(
        "--port", type=int, default=8050, help="Port to serve on (default: 8050)"
    )
    parser.add_argument("--debug", action="store_true", help="Enable Dash debug mode")
    args = parser.parse_args()

    # Validate env vars are present
    required = [
        "DISTRIBUTE_DB_HOST",
        "DISTRIBUTE_DB_NAME",
        "DISTRIBUTE_DB_USER",
        "DISTRIBUTE_DB_PASSWORD",
    ]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise RuntimeError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    app = create_app(poll_interval_s=args.poll_interval)
    app.run(host="0.0.0.0", port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
