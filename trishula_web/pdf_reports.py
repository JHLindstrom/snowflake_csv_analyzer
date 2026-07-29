"""Deterministic PDF reports for the selected dashboard tab."""

from io import BytesIO
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

MAX_BLOCKS = 600
MAX_TEXT_CHARS = 250_000
MAX_TABLE_CELLS = 20_000


def _safe_text(value):
    text = str(value or "").replace("→", "->").replace("…", "...")
    return escape(text)


def _validate_payload(payload):
    if not isinstance(payload, dict):
        raise ValueError("PDF request must be an object")
    blocks = payload.get("blocks")
    if not isinstance(blocks, list) or len(blocks) > MAX_BLOCKS:
        raise ValueError("PDF report contains too many content blocks")
    text_chars = 0
    table_cells = 0
    for block in blocks:
        if not isinstance(block, dict):
            raise ValueError("Invalid PDF content block")
        text_chars += len(str(block.get("text", "")))
        rows = block.get("rows", [])
        if rows:
            if not isinstance(rows, list):
                raise ValueError("Invalid PDF table")
            table_cells += sum(len(row) for row in rows if isinstance(row, list))
            text_chars += sum(
                len(str(cell))
                for row in rows
                if isinstance(row, list)
                for cell in row
            )
    if text_chars > MAX_TEXT_CHARS or table_cells > MAX_TABLE_CELLS:
        raise ValueError("PDF report exceeds the configured content limit")
    return blocks


def build_selected_tab_pdf(payload):
    """Build a bounded PDF from structured, visible dashboard content."""
    blocks = _validate_payload(payload)
    tab = str(payload.get("tab", "overview"))
    page_size = landscape(A4) if tab in {"heatmap", "search"} else A4
    output = BytesIO()
    document = SimpleDocTemplate(
        output,
        pagesize=page_size,
        leftMargin=14 * mm,
        rightMargin=14 * mm,
        topMargin=16 * mm,
        bottomMargin=16 * mm,
        title=str(payload.get("title", "Trishula report")),
        author="Trishula",
    )
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            "ReportTitle",
            parent=styles["Title"],
            textColor=colors.HexColor("#0f172a"),
            fontSize=19,
            leading=23,
            spaceAfter=8,
        )
    )
    styles.add(
        ParagraphStyle(
            "Meta",
            parent=styles["Normal"],
            textColor=colors.HexColor("#475569"),
            fontSize=8.5,
            leading=11,
        )
    )
    styles.add(
        ParagraphStyle(
            "BlockHeading",
            parent=styles["Heading2"],
            textColor=colors.HexColor("#0369a1"),
            fontSize=13,
            leading=16,
            spaceBefore=7,
            spaceAfter=5,
            keepWithNext=True,
        )
    )
    styles.add(
        ParagraphStyle(
            "BodyCompact",
            parent=styles["BodyText"],
            fontSize=9,
            leading=12,
            spaceAfter=4,
        )
    )
    styles.add(
        ParagraphStyle(
            "ReportCode",
            parent=styles["Code"],
            fontName="Courier",
            fontSize=7.5,
            leading=10,
            backColor=colors.HexColor("#f1f5f9"),
            borderPadding=6,
            spaceAfter=5,
        )
    )

    story = [
        Paragraph(_safe_text(payload.get("title", "Trishula report")), styles["ReportTitle"]),
        Paragraph(
            f"<b>Dataset:</b> {_safe_text(payload.get('dataset', 'No active dataset'))}",
            styles["Meta"],
        ),
        Paragraph(
            f"<b>Deduplication:</b> {_safe_text(payload.get('dedupe', '-'))}",
            styles["Meta"],
        ),
        Paragraph(
            f"<b>Generated:</b> {_safe_text(payload.get('generated_at', '-'))}",
            styles["Meta"],
        ),
        Spacer(1, 5 * mm),
    ]

    for block in blocks:
        kind = block.get("type")
        if kind == "heading":
            story.append(Paragraph(_safe_text(block.get("text")), styles["BlockHeading"]))
        elif kind in {"paragraph", "metric", "list-item", "status"}:
            prefix = "• " if kind == "list-item" else ""
            story.append(
                Paragraph(prefix + _safe_text(block.get("text")), styles["BodyCompact"])
            )
        elif kind == "code":
            story.append(Paragraph(_safe_text(block.get("text")), styles["ReportCode"]))
        elif kind == "page-break":
            story.append(PageBreak())
        elif kind == "table":
            rows = block.get("rows") or []
            if not rows:
                continue
            formatted = [
                [Paragraph(_safe_text(cell), styles["BodyCompact"]) for cell in row]
                for row in rows
            ]
            width = page_size[0] - document.leftMargin - document.rightMargin
            column_count = max(len(row) for row in rows)
            table = Table(
                formatted,
                colWidths=[width / column_count] * column_count,
                repeatRows=1,
                hAlign="LEFT",
            )
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#334155")),
                        ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 5),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                        ("TOPPADDING", (0, 0), (-1, -1), 4),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                    ]
                )
            )
            story.extend([table, Spacer(1, 4 * mm)])

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.setFont("Helvetica", 8)
        canvas.drawCentredString(page_size[0] / 2, 8 * mm, f"Page {doc.page}")
        canvas.restoreState()

    document.build(story, onFirstPage=footer, onLaterPages=footer)
    return output.getvalue()
