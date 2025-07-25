"""Weekly PDF report generator using ReportLab."""

import os
from datetime import datetime, timezone

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

import structlog

logger = structlog.get_logger(__name__)

OUTPUT_DIR = "reports/output"


def generate_weekly_report(
    top_repos: list[dict],
    language_trends: list[dict],
    top_contributors: list[dict],
) -> str:
    """Generate a weekly ecosystem health PDF report."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    now = datetime.now(timezone.utc)
    filename = f"{OUTPUT_DIR}/weekly_report_{now.strftime('%Y%m%d')}.pdf"

    doc = SimpleDocTemplate(filename, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = []

    # Title
    elements.append(Paragraph("GitHub Ecosystem Weekly Report", styles["Title"]))
    elements.append(Paragraph(f"Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}", styles["Normal"]))
    elements.append(Spacer(1, 0.3 * inch))

    # Top Repos
    elements.append(Paragraph("Top Repositories by Stars", styles["Heading2"]))
    repo_data = [["Repository", "Stars", "Forks", "Language"]]
    for r in top_repos[:15]:
        repo_data.append([r["full_name"], str(r["stars"]), str(r["forks"]), r.get("language", "N/A")])

    t = Table(repo_data, colWidths=[3 * inch, 1 * inch, 1 * inch, 1.5 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a73e8")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f5f5f5")]),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 0.3 * inch))

    # Language Trends
    elements.append(Paragraph("Language Popularity", styles["Heading2"]))
    lang_data = [["Language", "Repo Count", "Avg Stars"]]
    for lt in language_trends[:10]:
        lang_data.append([lt["language"], str(lt["count"]), str(lt["avg_stars"])])

    t2 = Table(lang_data, colWidths=[2 * inch, 2 * inch, 2 * inch])
    t2.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#34a853")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ]))
    elements.append(t2)

    doc.build(elements)
    logger.info("report_generated", path=filename)
    return filename
