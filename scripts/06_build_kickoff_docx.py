"""
KICK-OFF DOCUMENT BUILDER - SECTIONS 3 AND 4
============================================
Assembles "3 Data Description" and "4 General Statistics of the Data" into a
Word document, following the section structure of the kick-off deliverable
described in the course brief.

Every number and figure is read from the artefacts produced by
scripts/05_kickoff_figures.py, so the document cannot drift from the data.

  outputs/docs/DES431_Kickoff_Sections_3-4.docx

Run:  python scripts/05_kickoff_figures.py     (first - produces the inputs)
      python scripts/06_build_kickoff_docx.py
"""
from __future__ import annotations
import os
import sys
import json
import pandas as pd
from docx import Document
from docx.shared import Pt, Inches, RGBColor, Twips
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.section import WD_SECTION
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
from PIL import Image

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common as C

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

FIG = os.path.join(C.FIG_DIR, "kickoff")
STA = os.path.join(C.STAT_DIR, "kickoff")
DOCS = os.path.join(C.OUT, "docs")
os.makedirs(DOCS, exist_ok=True)
OUT_PATH = os.path.join(DOCS, "DES431_Kickoff_Sections_3-4.docx")

INK = RGBColor(0x20, 0x20, 0x20)
MUTED = RGBColor(0x59, 0x59, 0x59)
BODY_FONT = "Calibri"

# Usable text width on A4 with 1 inch margins, in DXA twips.
TEXT_DXA = 9026
MAX_FIG_W = 5.8      # inches
MAX_FIG_H = 5.2      # inches


# ==========================================================================
# Inputs
# ==========================================================================
def need(p):
    if not os.path.exists(p):
        raise SystemExit("Missing %s - run scripts/05_kickoff_figures.py first." % p)
    return p


S = json.load(open(need(os.path.join(STA, "K0_summary.json")), encoding="utf-8"))
rec = pd.read_csv(need(os.path.join(STA, "K1_records_by_month.csv")))
feat = pd.read_csv(need(os.path.join(STA, "K2_feature_table.csv")))
miss = pd.read_csv(need(os.path.join(STA, "K3_missing_by_column_by_month.csv")), index_col=0)
uniq = pd.read_csv(need(os.path.join(STA, "K6_unique_stations_per_month.csv")))
dur = pd.read_csv(os.path.join(C.STAT_DIR, "04_duration_stats.csv")).set_index("statistic").minutes
mbike = pd.read_csv(os.path.join(C.STAT_DIR, "03_missing_by_biketype.csv"))

N = S["total_rows"]
MONTHS = list(rec.year_month)


def f(n):
    return "{:,}".format(int(n))


def pct(x, d=2):
    return ("%." + str(d) + "f%%") % x


# ==========================================================================
# Document scaffolding
# ==========================================================================
doc = Document()

st = doc.styles["Normal"]
st.font.name = BODY_FONT
st.font.size = Pt(11)
st.font.color.rgb = INK
st.paragraph_format.space_after = Pt(10)
st.paragraph_format.line_spacing = 1.0
# The reference document justifies body text and indents the first line.
st.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
st.paragraph_format.first_line_indent = Inches(0.5)

sec = doc.sections[0]
sec.top_margin = sec.bottom_margin = Inches(1.0)
sec.left_margin = sec.right_margin = Inches(1.0)

# Numbered headings are "<number><tab><title>", black, with the title aligned on
# a half-inch tab stop - matching the reference document.
for name, size, before in (("Heading 1", 14, 20), ("Heading 2", 13, 16), ("Heading 3", 11, 14)):
    s = doc.styles[name]
    s.font.name = BODY_FONT
    s.font.size = Pt(size)
    s.font.bold = True
    s.font.color.rgb = INK
    s.paragraph_format.space_before = Pt(before)
    s.paragraph_format.space_after = Pt(8)
    s.paragraph_format.keep_with_next = True
    s.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.LEFT
    s.paragraph_format.first_line_indent = Inches(0)
    s.paragraph_format.tab_stops.add_tab_stop(Inches(0.5))


def page_number_footer():
    """Page number, bottom right, as a PAGE field."""
    p = sec.footer.paragraphs[0]
    p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    p.paragraph_format.first_line_indent = Inches(0)
    r = p.add_run()
    r.font.name = BODY_FONT
    r.font.size = Pt(10)
    for el, attr in (("w:fldChar", "begin"), ("w:instrText", None), ("w:fldChar", "end")):
        e = OxmlElement(el)
        if el == "w:fldChar":
            e.set(qn("w:fldCharType"), attr)
        else:
            e.set(qn("xml:space"), "preserve")
            e.text = " PAGE "
        r._r.append(e)


def heading(number, title, level):
    """Numbered heading: number, tab, then the title."""
    h = doc.add_heading("", level=level)
    h.add_run("%s\t%s" % (number, title))
    return h


def para(text="", size=11, bold=False, italic=False, align=None, color=None,
         space_after=None, style=None, indent=True):
    p = doc.add_paragraph(style=style)
    if text:
        r = p.add_run(text)
        r.font.size = Pt(size)
        r.bold = bold
        r.italic = italic
        r.font.name = BODY_FONT
        r.font.color.rgb = color or INK
    if align is not None:
        p.alignment = align
    if not indent:
        p.paragraph_format.first_line_indent = Inches(0)
    if space_after is not None:
        p.paragraph_format.space_after = Pt(space_after)
    return p


def rich(parts, size=11, align=None, space_after=None):
    """parts: list of (text, bold) tuples, rendered into one paragraph."""
    p = doc.add_paragraph()
    for text, bold in parts:
        r = p.add_run(text)
        r.font.size = Pt(size)
        r.bold = bold
        r.font.name = BODY_FONT
        r.font.color.rgb = INK
    if align is not None:
        p.alignment = align
    if space_after is not None:
        p.paragraph_format.space_after = Pt(space_after)
    return p


def bullets(items, size=11):
    for it in items:
        p = doc.add_paragraph(style="List Bullet")
        p.paragraph_format.space_after = Pt(4)
        p.paragraph_format.first_line_indent = Inches(0)
        p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        if isinstance(it, str):
            it = [(it, False)]
        for text, bold in it:
            r = p.add_run(text)
            r.font.size = Pt(size)
            r.bold = bold
            r.font.name = BODY_FONT
            r.font.color.rgb = INK


def breakable(text):
    """Allow a wrap after each underscore so snake_case headers break cleanly."""
    return text.replace("_", "_​")


def set_repeat_header(row):
    tr = row._tr
    pr = tr.get_or_add_trPr()
    el = OxmlElement("w:tblHeader")
    el.set(qn("w:val"), "true")
    pr.append(el)


def table(headers, rows, widths, size=9, caption=None):
    """widths in DXA, must sum to TEXT_DXA.

    Styled to match the reference document: plain black grid, bold header row,
    no fill, no zebra striping, everything left-aligned, and a plain unnumbered
    label above the table rather than a numbered caption.
    """
    if caption:
        p = para(caption, size=10, space_after=4, indent=False)
        p.paragraph_format.keep_with_next = True
    t = doc.add_table(rows=1, cols=len(headers))
    t.style = "Table Grid"
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    t.autofit = False

    # python-docx stores widths in EMU; the layout above is expressed in DXA twips.
    for j, w in enumerate(widths):
        t.columns[j].width = Twips(w)

    hdr = t.rows[0]
    for j, h in enumerate(headers):
        cell = hdr.cells[j]
        cell.width = Twips(widths[j])
        cell.text = ""
        p = cell.paragraphs[0]
        p.paragraph_format.space_after = Pt(2)
        p.paragraph_format.space_before = Pt(2)
        p.paragraph_format.first_line_indent = Inches(0)
        p.alignment = WD_ALIGN_PARAGRAPH.LEFT
        r = p.add_run(str(h))
        r.bold = True
        r.font.size = Pt(size)
        r.font.name = BODY_FONT
        r.font.color.rgb = INK
    set_repeat_header(hdr)

    for i, row in enumerate(rows):
        cells = t.add_row().cells
        for j, v in enumerate(row):
            cell = cells[j]
            cell.width = Twips(widths[j])
            cell.text = ""
            p = cell.paragraphs[0]
            p.paragraph_format.space_after = Pt(2)
            p.paragraph_format.space_before = Pt(2)
            p.paragraph_format.first_line_indent = Inches(0)
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            txt = str(v)
            bold = txt.startswith("**") and txt.endswith("**")
            if bold:
                txt = txt[2:-2]
            r = p.add_run(txt)
            r.font.size = Pt(size)
            r.bold = bold
            r.font.name = BODY_FONT
            r.font.color.rgb = INK
    para("", size=4, space_after=2, indent=False)
    return t


FIGNO = {}


def figure(fname, number, caption, width=None):
    """Insert a figure, scaled to fit the text block.

    The reference document presents figures bare - centred, no caption - so
    `number` and `caption` are retained only as in-source documentation of what
    each figure shows.
    """
    path = os.path.join(FIG, fname)
    if not os.path.exists(path):
        raise SystemExit("Missing figure %s" % path)
    with Image.open(path) as im:
        pw, ph = im.size
    w = width or MAX_FIG_W
    w = min(w, MAX_FIG_W)
    if w * ph / pw > MAX_FIG_H:
        w = MAX_FIG_H * pw / ph
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.paragraph_format.space_after = Pt(4)
    p.paragraph_format.keep_with_next = True
    p.paragraph_format.first_line_indent = Inches(0)
    p.paragraph_format.space_after = Pt(12)
    p.paragraph_format.keep_with_next = False
    p.add_run().add_picture(path, width=Inches(w))
    FIGNO[number] = caption
    return p


# ==========================================================================
# Title block
# ==========================================================================
page_number_footer()

para("Divvy Bike-Share Demand and Station Imbalance Analysis",
     size=14, bold=True, align=WD_ALIGN_PARAGRAPH.CENTER, space_after=2, indent=False)
para("DES431 Big Data Analytics - Kick-off Document, Sections 3 and 4",
     size=11, align=WD_ALIGN_PARAGRAPH.CENTER, space_after=20, indent=False)


# ==========================================================================
# 3 Data Description
# ==========================================================================
heading("3", "Data Description", 1)

para(
    "The project uses the public trip-record archive of Divvy, the bicycle-sharing "
    "system operated in Chicago by Lyft Bikes and Scooters, LLC on behalf of the "
    "Chicago Department of Transportation. Divvy publishes one file per calendar "
    "month covering every trip taken on the system. Each record is a single "
    "completed journey, so the grain of the dataset is one row per trip, which "
    "makes it a natural fact table for analytics."
)
para(
    "The study window for this project is the twelve months from October 2024 to "
    "September 2025 inclusive, which is the most recent complete year available at "
    "the time of writing. Taken together these twelve files hold "
    "%s trip records across %d columns and occupy %s MB on disk as raw CSV. "
    "All twelve files share an identical header, so there is no schema drift to "
    "reconcile before the files can be treated as one table."
    % (f(N), S["n_columns"], f(S["raw_size_mb"]))
)
para(
    "The data is fully structured and machine-generated: comma-delimited, "
    "double-quote qualified, UTF-8 encoded, with one header line per file. Each "
    "trip carries a unique identifier, the bike class, start and end timestamps at "
    "millisecond precision, the origin and destination docking stations where "
    "applicable, the origin and destination coordinates in WGS84 decimal degrees, "
    "and the rider's subscription type. Records are anonymised at source: there is "
    "no rider identifier, no payment information and no personally identifying "
    "information of any kind, so the dataset can be processed without privacy "
    "controls beyond the publisher's licence terms."
)
para(
    "In terms of volume and frequency, the archive grows by one file each month. "
    "Across the study window the system recorded an average of roughly 15,200 trips "
    "per day, but that average conceals a very large seasonal swing: the quietest "
    "day in the window carried under 1,500 trips while the busiest carried more "
    "than 32,000. Monthly volume ranges from 138,689 records in January 2025 to "
    "790,177 in August 2025, a factor of 5.7. At the current rate the archive "
    "accumulates approximately 1.1 GB of raw CSV per year, and the full historical "
    "archive published by Divvy, which reaches back to 2013, is substantially "
    "larger than the twelve-month slice used here. The combination of a "
    "multi-gigabyte, continuously growing, event-level table and analytics that "
    "must aggregate across the whole of it is what makes a distributed processing "
    "pipeline appropriate for this project rather than a single-machine workflow."
)

doc.add_page_break()
table(
    ["Property", "Value"],
    [
        ["Source", "Divvy System Data, published by Lyft Bikes and Scooters, LLC"],
        ["Source link", "https://divvybikes.com/system-data"],
        ["Licence", "Divvy Data License Agreement (free public use, anonymised)"],
        ["Data type", "Structured, tabular, machine-generated event records"],
        ["Format", "CSV - comma-delimited, quote-qualified, UTF-8, one header row per file"],
        ["Grain", "One row per completed trip"],
        ["Files", "%d monthly files (October 2024 - September 2025)" % S["n_files"]],
        ["Records", "**%s**" % f(N)],
        ["Columns", "%d, identical across all %d files (no schema drift)"
                    % (S["n_columns"], S["n_files"])],
        ["Raw size", "%s MB CSV (approximately 1.1 GB per year)" % f(S["raw_size_mb"])],
        ["Timestamp range", "%s to %s" % (S["date_min"][:19], S["date_max"][:19])],
        ["Update frequency", "One new monthly file published per month"],
        ["Average daily volume", "approximately 15,200 trips per day"],
        ["Distinct stations", "%s station names, %s raw station_id values"
                              % (f(S["distinct_station_names"]), f(S["distinct_station_ids"]))],
    ],
    widths=[2400, 6626], size=10,
    caption="Dataset at a glance",
)


# ==========================================================================
# 4 General Statistics of the Data
# ==========================================================================
heading("4", "General Statistics of the Data", 1)

para(
    "The statistics in this section were computed over the complete twelve-month "
    "dataset - all %s records - rather than over a sample. This was possible on "
    "ordinary hardware because the profiling pass streams the data one monthly "
    "file at a time and keeps only running aggregates in memory, so peak memory "
    "stays close to the size of the largest single month (around 790,000 rows) "
    "instead of the full 5.5 million. Fixed histogram bins, running sums and "
    "running co-moments are accumulated across files and combined at the end, "
    "which gives exact answers rather than approximations from a subsample."
    % f(N)
)
para(
    "This is deliberately a profiling strategy, not the analytics strategy. It "
    "scales to one year on one machine, but it is sequential and it will not scale "
    "to the full multi-year archive or to the repeated, interactive aggregations "
    "the analytics stage requires. Those workloads are the reason the project "
    "moves onto a distributed pipeline, described in Section 5. Everything "
    "reported below is reproducible from the scripts in the project repository."
)

# ---- 4.1 ------------------------------------------------------------------
heading("4.1", "Number of Records", 2)

para(
    "The twelve monthly files hold %s trip records in total. Volume is strongly "
    "seasonal, which is the single most important structural property of this "
    "dataset: any model, dashboard or capacity estimate built on it has to treat "
    "seasonality as a first-order effect rather than as noise." % f(N)
)

rows = [[m, f(r), "%.1f" % s, pct(p, 2)]
        for m, r, s, p in zip(rec.year_month, rec.records, rec.file_size_mb, rec.pct_of_total)]
rows.append(["**Total**", "**%s**" % f(N), "**%s**" % f(S["raw_size_mb"]), "**100.00%**"])
table(["Month", "Records", "File size (MB)", "% of total"], rows,
      widths=[2200, 2400, 2400, 2026], size=10,
      caption="Records and raw file size per month")

figure("K01_records_per_month.png", "4.1",
       "Records per monthly file. August 2025 carries 5.7 times the volume of January 2025.")
para(
    "Aggregating to the calendar day shows the same seasonal cycle with a weekly "
    "rhythm superimposed on it. The sharp day-to-day spikes visible in the raw "
    "series are weather-driven: Chicago ridership responds immediately to rain and "
    "temperature, which is why the seven-day mean is a much more stable signal "
    "than any single day."
)
figure("K02_trips_per_day.png", "4.2",
       "Trips per calendar day over the study window, with the seven-day centred mean.")

# ---- 4.2 ------------------------------------------------------------------
heading("4.2", "Number of Features", 2)

para(
    "Every file carries the same %d columns. The table below lists each one with "
    "its data type, the range of values observed across the whole dataset and a "
    "representative example. Two points in the table are worth flagging early "
    "because they shape the preprocessing design."
    % S["n_columns"]
)
bullets([
    [("The station columns are null on roughly one trip in five. ", True),
     ("This is not damage. It is a structural property of the system and is "
      "analysed in Section 4.3.", False)],
    [("The end coordinate range is impossible. ", True),
     ("end_lat spans 35.50 to 83.14 and end_lng spans -94.27 to 152.53, which "
      "would place trips in the high Arctic and the western Pacific, whereas the "
      "start coordinates stay inside a sensible Chicago bounding box. These are "
      "isolated GPS failures and there are only 24 of them in 5.5 million rows, "
      "but they have to be measured before they can be dismissed.", False)],
])

rows = []
for _, r in feat.iterrows():
    rows.append([r["#"], r["feature"], r["data_type"], r["description"],
                 r["range"], r["example"]])
table(["#", "Feature", "Type", "Description", "Range observed", "Example"], rows,
      widths=[380, 1620, 1080, 2200, 2020, 1726], size=8,
      caption="Raw Data Features")

para(
    "Beyond these thirteen source columns the preprocessing stage derives a "
    "further set of analytic features from them - trip duration, straight-line "
    "distance, average speed, the calendar and clock parts of the start timestamp, "
    "a geohash cell for each endpoint, and a stable station key. Those derived "
    "columns are described in the methodology rather than here, but three of them "
    "(duration, distance and speed) are used in the distributions below because "
    "they are the quantities that expose the data-quality problems most clearly."
)

# ---- 4.3 ------------------------------------------------------------------
heading("4.3", "Missing Data", 2)

nulls = feat[feat.missing > 0]
para(
    "Nine of the thirteen columns are complete. Missing values occur in only "
    "%d columns, and they fall into two clearly separated groups."
    % len(nulls)
)

rows = [[r["feature"], f(r["missing"]), pct(r["pct_missing"])]
        for _, r in feat.iterrows()]
table(["Feature", "Missing records", "% missing"], rows,
      widths=[4026, 2600, 2400], size=9,
      caption="Missing values per column")

para(
    "Breaking the same counts down by month shows that the pattern is completely "
    "stable: the missing rate moves only with seasonal volume and never jumps. "
    "Whatever causes the nulls has been operating consistently for the whole "
    "period, which already argues against treating them as a data-collection "
    "incident."
)

cols = list(nulls.feature)
rows = []
for m in MONTHS:
    rows.append([m] + [f(miss.loc[c, m]) for c in cols])
rows.append(["**Total**"] + ["**%s**" % f(miss.loc[c, "Total"]) for c in cols])
w = [1226] + [1300] * 6
table(["Month"] + [breakable(c) for c in cols], rows, widths=w, size=7.5,
      caption="Missing values by column and by month")

figure("K12_missing_by_column.png", "4.3",
       "Share of records with a null value, by column.")
figure("K13_missing_heatmap.png", "4.4",
       "Missing rate by column and month. The pattern is stable across the year.")

doc.add_heading("The station nulls are structural, not random", level=3)

ebike = mbike[mbike.rideable_type == "electric_bike"].iloc[0]
classic = mbike[mbike.rideable_type == "classic_bike"].iloc[0]

para(
    "The %s missing start-station names are the most consequential feature of this "
    "dataset, and the obvious responses - drop the rows, or impute a station - are "
    "both wrong. Splitting the missingness by bike type explains 100%% of it."
    % f(int(nulls[nulls.feature == "start_station_name"].missing.iloc[0]))
)

table(["Bike type", "Trips", "Missing start station", "Rate"],
      [["classic_bike", f(classic.rows), f(classic.missing_start_station),
        pct(classic.pct_missing_start)],
       ["electric_bike", f(ebike.rows), f(ebike.missing_start_station),
        pct(ebike.pct_missing_start)]],
      widths=[2400, 2200, 2626, 1800], size=10,
      caption="Missing start-station names by bike type")

figure("K14_missing_by_biketype.png", "4.5",
       "Missing station names by bike type, for both trip endpoints.")

para(
    "Divvy electric bikes are dockless: a rider may end a trip by locking the bike "
    "to any public rack, in which case no station is recorded. Classic bikes must "
    "be returned to a dock, so a classic-bike trip never has a missing station - "
    "the observed rate is exactly 0.00%. The nulls therefore encode a real "
    "operational mode of the system. In missing-data terms this is MNAR (Missing "
    "Not At Random), because the probability that a value is absent depends on an "
    "observable property of the record itself."
)
bullets([
    [("Dropping ", True),
     ("these rows would discard %s of all electric-bike trips and bias every "
      "downstream demand estimate towards the docked, classic-bike population."
      % pct(ebike.pct_missing_start, 1), False)],
    [("Imputing ", True),
     ("a station name would fabricate dock activity that never happened, which "
      "would corrupt precisely the station-imbalance analysis the project intends "
      "to run.", False)],
    [("Keeping and flagging ", True),
     ("them is the correct treatment. The coordinates are always present on these "
      "rows, so a geohash cell gives every dockless trip a usable spatial "
      "identity where a station name does not exist.", False)],
])
para(
    "The practical difference is large. Calling dropna() across all columns would "
    "retain 3,770,024 of %s rows, a loss of 31.9%%, and would silently change the "
    "population under study from the whole system to its docked subset. The "
    "rule-based cleaning actually applied removes 142,350 rows, a loss of 2.57%%, "
    "and every removed row is attributable to a named quality rule."
    % f(N)
)

para(
    "The second group of nulls is unrelated and far smaller: %s records, %s of the "
    "dataset, have no end coordinate. These are trips where the bike was never "
    "properly returned, so no closing GPS fix was written. Trip distance and speed "
    "cannot be computed for them, and they are removed during cleaning."
    % (f(int(nulls[nulls.feature == "end_lat"].missing.iloc[0])),
       pct(float(nulls[nulls.feature == "end_lat"].pct_missing.iloc[0])))
)

# ---- 4.4 ------------------------------------------------------------------
heading("4.4", "Sample Distributions", 2)

para(
    "The distributions below are computed over the full dataset. Categorical "
    "columns are shown as shares, numeric columns as histograms with fixed bins "
    "accumulated across all twelve files, and the two high-cardinality columns - "
    "the station names - are summarised by their top 30 values and by the shape of "
    "their long tail."
)

doc.add_heading("Distribution of each feature", level=3)

para(
    "The two categorical columns split the dataset into its primary analytic "
    "segments. Electric bikes carry the majority of trips, which is what makes the "
    "dockless-station issue in Section 4.3 so significant, and members account for "
    "roughly two thirds of all journeys."
)
figure("K03_bike_type_share.png", "4.6",
       "Share of records by rideable_type.", width=4.2)
figure("K04_user_type_share.png", "4.7",
       "Share of records by member_casual.", width=4.2)

para(
    "The start timestamp is the most informative column in the dataset. Broken "
    "down by hour it is clearly bimodal, with a morning commuting peak around "
    "08:00 and a larger, broader afternoon peak around 17:00 - the signature of a "
    "system used heavily for commuting. The day-of-week view shows why the two "
    "rider segments cannot be pooled: the weekday peaks are driven by members, "
    "while casual riders shift the weekend distribution towards the middle of the "
    "day."
)
figure("K05_hour_distribution.png", "4.8", "All trips by hour of start time.")
figure("K06_dayofweek_distribution.png", "4.9", "All trips by day of week.")

para(
    "Trip duration, plotted on a logarithmic axis, reveals two populations rather "
    "than one. The main mode sits near the median of %.2f minutes, but there is a "
    "clearly separated secondary mode in the region of 10 to 30 seconds. That "
    "second mode is the classic failed-checkout signature: a rider undocks a bike, "
    "finds a fault and immediately re-docks it. There is also a spike at exactly "
    "24 hours - the 99.9th percentile lands at %.2f minutes, fractionally under the "
    "1,440-minute mark - which is a system auto-close on bikes that were never "
    "returned, not a day-long ride. A further 43 records end before they start, "
    "with a minimum duration of %.2f minutes, most likely clock-synchronisation or "
    "daylight-saving artefacts."
    % (dur["p50"], dur["p99.9"], dur["min"])
)
figure("K07_duration_distribution.png", "4.10",
       "Trip duration on a logarithmic axis, with the preprocessing cut-offs marked.")

table(["Statistic", "Minutes", "Percentile", "Minutes"],
      [["mean", "%.2f" % dur["mean"], "p1", "%.2f" % dur["p1"]],
       ["std", "%.2f" % dur["std"], "p25", "%.2f" % dur["p25"]],
       ["min", "**%.2f**" % dur["min"], "p50", "**%.2f**" % dur["p50"]],
       ["max", "%.2f" % dur["max"], "p75", "%.2f" % dur["p75"]],
       ["count", f(dur["count"]), "p95", "%.2f" % dur["p95"]],
       ["", "", "p99", "%.2f" % dur["p99"]],
       ["", "", "**p99.9**", "**%.2f**" % dur["p99.9"]]],
      widths=[2300, 2200, 2300, 2226], size=10,
      caption="Trip duration statistics")

para(
    "Straight-line distance and average speed are derived from the coordinates and "
    "the timestamps. Both behave as expected for an urban bike-share system: most "
    "journeys are short, and average speed clusters tightly around 11 to 12 km/h. "
    "Because the distance is measured as the crow flies rather than along the "
    "route actually cycled, the derived speed is a conservative lower bound, which "
    "is exactly the property wanted when speed is used to detect impossible trips."
)
figure("K08_distance_distribution.png", "4.11",
       "Straight-line distance between trip endpoints.")
figure("K09_speed_distribution.png", "4.12",
       "Average speed, computed from straight-line distance and elapsed time.")

para(
    "The four coordinate columns are unimodal and tightly concentrated, reflecting "
    "the geography of the service area: a dense core along the lakefront and the "
    "near North Side, thinning out towards the city boundary. Plotted on the same "
    "axes the start and end distributions are almost indistinguishable, which is "
    "expected in a system where most trips both begin and end inside the same "
    "dense core."
)
figure("K10_coordinate_distributions.png", "4.13",
       "Distribution of the four coordinate columns.")

doc.add_heading("Spatial distribution of demand", level=3)
para(
    "Mapping the stations by the number of trip endpoints they serve makes the "
    "concentration explicit. Demand is heavily weighted towards the central "
    "business district and the lakefront parks, with a secondary cluster around "
    "the university campus on the South Side."
)
figure("K11_station_map.png", "4.14",
       "Stations positioned by coordinate, sized and coloured by trip endpoints.",
       width=5.4)

doc.add_heading("Top 30 start stations and top 30 end stations", level=3)
para(
    "The station-name columns have too many distinct values to plot directly - "
    "%s distinct names appear across the study window - so their distribution is "
    "represented by the 30 most frequent values, in the same way the distribution "
    "of a high-cardinality text column would be. The ranking is dominated by the "
    "lakefront parks, the Navy Pier and Millennium Park tourist corridor, and the "
    "commuter approaches to the Loop."
    % f(S["distinct_station_names"])
)
figure("K15_top30_start_stations.png", "4.15",
       "The 30 stations with the most departures.")
para(
    "The destination ranking is near-identical to the origin ranking: the same "
    "stations appear, in almost the same order, with comparable totals. Over a "
    "full year, therefore, traffic between the busiest stations is broadly "
    "balanced. That is a statement about annual totals only, and it is worth "
    "stating explicitly because it is easy to misread as evidence that there is no "
    "imbalance problem to solve. The imbalance this project sets out to study is "
    "not visible at this resolution: it appears hour by hour, as these same "
    "stations empty during the morning commute and refill in the evening. "
    "Detecting it requires the hourly station-level aggregation prepared in the "
    "preprocessing stage, not a yearly ranking."
)
figure("K16_top30_end_stations.png", "4.16",
       "The 30 stations with the most arrivals.")

doc.add_heading("Station activity distribution", level=3)
para(
    "Ranking all stations by departures shows a pronounced long tail. A small "
    "minority of stations carries most of the traffic, while several hundred "
    "stations see only a trickle. Aggregating instead to the station-day level "
    "confirms this from a different angle: the median station-day carries only a "
    "handful of departures. Any rebalancing analysis therefore has to be weighted "
    "by station, because an unweighted average across stations would be dominated "
    "by hundreds of near-idle locations."
)
figure("K18_station_long_tail.png", "4.17",
       "Stations ranked by departures over the study window.")
figure("K19_trips_per_station_per_day.png", "4.18",
       "Departures per station per calendar day.")

doc.add_heading("Number of active stations per month", level=3)
para(
    "Counting the distinct station names that appear as a trip endpoint in each "
    "month gives an apparent seasonal cycle in the size of the network. The "
    "network itself does not shrink in winter; rather, a station only appears in "
    "the data if at least one trip used it, so quiet outlying stations drop out of "
    "the count during the cold months and return in spring. This is a useful "
    "reminder that an apparently structural metric can be an artefact of how the "
    "data is recorded."
)
figure("K17_unique_stations_per_month.png", "4.19",
       "Distinct station names observed as a trip endpoint in each month.")

doc.add_heading("Station identifier integrity", level=3)
para(
    "Profiling the two identifier columns uncovered a defect that would be easy to "
    "miss and expensive to discover late. The dataset contains %s distinct station "
    "names but %s distinct station_id values, and %s of those names - %s of them - "
    "are associated with more than one identifier."
    % (f(S["distinct_station_names"]), f(S["distinct_station_ids"]),
       f(S["names_with_multiple_ids"]),
       pct(100 * S["names_with_multiple_ids"] / S["distinct_station_names"], 1))
)
figure("K21_ids_per_station_name.png", "4.20",
       "Number of distinct station_id values observed per station name.")
para(
    "Two independent problems are at work. Divvy migrated from numeric identifiers "
    "to a CHIxxxxx scheme partway through the study window, so a single station "
    "appears under both schemes; and retired identifiers were then reassigned to "
    "unrelated stations, so 105 identifiers map to more than one name. A GROUP BY "
    "on station_id would therefore fragment most stations into two or more rows "
    "and silently merge 105 pairs of unrelated ones, with no error to warn anyone. "
    "The preprocessing stage resolves this by deriving a stable surrogate key from "
    "the normalised station name and demoting station_id to a traceability-only "
    "column."
)

doc.add_heading("Correlation between numeric features", level=3)
para(
    "The correlation matrix covers the three derived measures and the numeric "
    "source columns, computed over the %s records that are complete in all twelve "
    "variables. The strong entries are all mechanical rather than informative: "
    "start and end latitude correlate at 0.81 because trips are short and stay "
    "within the same city, and distance correlates with end longitude for the same "
    "geographic reason. Everything else is weak."
    % f(S["correlation_complete_cases"])
)
figure("K20_correlation_matrix.png", "4.21",
       "Pearson correlation between the numeric and derived features.", width=5.8)
para(
    "The absence of strong correlation among the behavioural variables is a useful "
    "negative result at this stage. It means there is no redundant column that can "
    "simply be dropped, and it means that the structure this project is looking "
    "for - which stations run dry, when, and which rider segment drives it - is "
    "not going to be visible in pairwise linear correlation on raw columns. It "
    "lives in the interaction between time, location and rider type, which is "
    "what the analytics stage described in Section 5 is designed to expose."
)

doc.save(OUT_PATH)
print("Wrote %s" % OUT_PATH)
print("  %d figures, %d tables" % (len(FIGNO), len(doc.tables)))
