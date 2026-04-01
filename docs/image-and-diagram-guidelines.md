# Image and diagram guidelines (NYC TLC portfolio)

Use this when **adding or updating** architecture figures, README hero images, or related assets so they stay **factually aligned** with this repository.

## 1. Facts — do not invent

- **NYC TLC track — source data:** Monthly **Parquet** from NYC TLC (Yellow/Green), **not** Excel/CSV as the primary story.
- **NYC TLC — local processing:** **Python + PyArrow** (merge monthly files, schema alignment) before cloud load. **DuckDB is not** the TLC ingest path in this repo.
- **DuckDB / dlt** appear on the **GitHub Archive** track only (separate flow). If the diagram is **TLC-only**, omit DuckDB or label it “other track.”
- **Order:** Download → (optional **Kestra** orchestration of the same script) → **GCS** (lake) → **BigQuery** load with **partitioning + clustering** → **dbt** (staging → core → mart) → **Looker Studio** on **mart** tables.
- **Scope:** Yellow/Green taxi, **2019–2020** batch is the documented example.

## 2. What to draw (recommended)

| Goal | Suggestion |
|------|------------|
| **Accurate pipeline** | Left-to-right **swimlane** or **flowchart**: External TLC → Ingest (Python) → GCS → BigQuery → dbt layers → Looker. Optional small “Kestra” box wrapping ingest steps. |
| **Classic DW comparison** | Only as a **small inset** or separate figure; label it “conceptual.” The **canonical** truth for this repo is **GCS + BQ + dbt**, not a single cylinder labeled only “DW.” |
| **Second track** | If showing the **whole repo**, add a **parallel branch**: GitHub Archive → DuckDB/sample → GCS → BigQuery (subset), clearly separate from TLC. |

## 3. Style & deliverables

- **Readable on GitHub:** High contrast, **large labels**, few fonts; avoid tiny text.
- **Formats:** **SVG** or **PNG** (≥1200px wide for hero images). Prefer SVG if the tool supports editable layers.
- **Colors:** Neutral professional (e.g. blue = cloud/storage, green = warehouse, orange = transform, purple = BI)—or match Looker/GCP palette loosely; stay consistent.
- **No misleading stock metaphors:** Avoid “Excel warehouse,” “single monolithic database” if the story is **lake + warehouse + dbt**.

## 4. Optional: English brief for illustrators

Use this block as a **handoff** to a designer or external illustrator (copy as-is or adapt):

```
You are illustrating a documented data engineering portfolio repo. Follow these constraints strictly:

PIPELINE (NYC TLC track only):
1) Source: NYC TLC official Parquet (Yellow and Green), monthly files — NOT Excel.
2) Ingest: Python script + PyArrow merge; optionally orchestrated by Kestra (same script stages: download → upload → BigQuery).
3) Data lake: Google Cloud Storage (GCS).
4) Data warehouse: Google BigQuery — partitioned and clustered trip tables.
5) Transform: dbt — staging → core → mart (not raw SQL only).
6) BI: Looker Studio connected to mart tables.

Do NOT label DuckDB or DuckDB-based ingestion for the TLC path. DuckDB belongs to a separate “GitHub Archive” track if you draw two lanes.

Output: one clear left-to-right architecture diagram, labels in English, suitable for a GitHub README. Add a one-line caption: “NYC TLC track — batch 2019–2020 (conceptual).”
```

## 5. Optional: Mermaid instead of raster

If a clean **SVG** export is impractical, use **Mermaid** (`flowchart LR` or `graph TB`) in the README that matches the same steps—GitHub renders Mermaid natively.

## 6. Reference asset in this repo

A **baseline** diagram aligned with these guidelines lives at **`docs/nyc-tlc-pipeline-architecture.png`**. Treat it as a starting point; refine labels or style in Figma, Excalidraw, or similar if you need pixel-perfect branding.
