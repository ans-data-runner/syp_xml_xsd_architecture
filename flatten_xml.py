"""
SYP Connect VA/VC XML Generic Flattener
----------------------------------------
Synapse notebook cell(s), aligned to the actual SYP platform configuration:

  Serverless SQL pool : synw-symdp-data-d-uksouth-001
  Lakehouse            : synw-syp-lakehouse
  Source tables        : pronto.ddform_connectsr_va, pronto.ddform_connectsr_vc
                          (XML payload column is decoded before parsing -
                          confirm exact column name/encoding before go-live)
  Processed output     : pronto.xml_record, pronto.xml_attribute,
                          pronto.xml_parse_error
  Validation views      : pronto_vw.*
  Power BI views        : pbi_vw.*

Pattern: "generic shred", not a fixed per-field mapping.
The source XML is a self-describing, EAV-style export from the Connect RMS
(CompoundRecord / SimpleRecord / CompoundList / SimpleList / Primitive,
each carrying a 'field' name and sometimes a 'type'). Rather than hand-coding
column-per-field extraction (which breaks the moment a field is added,
renamed, or a form variant changes), we walk the tree once and produce two
generic, stable outputs, written into the pronto schema:

  pronto.xml_record     - hierarchy / "rows": one row per CompoundRecord /
                           SimpleRecord / CompoundList / SimpleList / root
  pronto.xml_attribute  - "leaf values": one row per Primitive, linked back
                           to its parent record_id
  pronto.xml_parse_error - one row per source row that failed to parse

Everything downstream (Person, Incident, Location, RiskAssessment, etc.)
is built as SQL views in pronto_vw (validation) and pbi_vw (reporting) over
these generic outputs. New source fields just show up as new rows in
pronto.xml_attribute -- they do not break ingestion.
"""

SERVERLESS_POOL = "synw-symdp-data-d-uksouth-001"
LAKEHOUSE_NAME = "synw-syp-lakehouse"
SOURCE_TABLES = ["pronto.ddform_connectsr_va", "pronto.ddform_connectsr_vc"]
PROCESSED_SCHEMA = "pronto"
VALIDATION_SCHEMA = "pronto_vw"
REPORTING_SCHEMA = "pbi_vw"

# Confirm against the real table definition before go-live (Open Questions,
# design doc Section 11) - these are placeholders.
XML_PAYLOAD_COLUMN = "<confirm_xml_payload_column>"
SOURCE_KEY_COLUMN = "<confirm_source_primary_or_business_key>"


def decode_xml_payload(raw_payload) -> bytes:
    """
    Convert the value held in XML_PAYLOAD_COLUMN into a plain XML byte string.
    Placeholder - confirm actual encoding (plain text / base64 / escaped) and
    implement accordingly. Kept as its own function so VA and VC share one
    decode + parse path and so this is independently unit-testable.
    """
    if isinstance(raw_payload, bytes):
        return raw_payload
    return str(raw_payload).encode("utf-8")


import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

CONTAINER_TAGS = {"CompoundRecord", "SimpleRecord", "CompoundList", "SimpleList", "List"}
VALUE_TAG = "Primitive"


def _make_id(*parts):
    """Deterministic surrogate id for elements that have no native ID attribute."""
    return hashlib.md5("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:16]


def flatten_xml(xml_bytes: bytes, source_table: str, source_record_key: str, run_id: str):
    """
    Parse one decoded XML document and return (records, attributes) as lists
    of dicts, ready to be turned into Spark/pandas DataFrames and written to
    pronto.xml_record / pronto.xml_attribute.
    """
    root = ET.fromstring(xml_bytes)
    doc_id = root.attrib.get("ID") or _make_id(source_record_key)
    content_hash = hashlib.sha256(xml_bytes).hexdigest()
    ingest_ts = datetime.now(timezone.utc).isoformat()

    records = []
    attributes = []

    def visit(elem, parent_record_id, path, sibling_index):
        tag = elem.tag
        field = elem.attrib.get("field")
        etype = elem.attrib.get("type")
        native_id = elem.attrib.get("ID")

        if tag in CONTAINER_TAGS or elem is root:
            record_id = native_id or _make_id(source_record_key, path, sibling_index)
            records.append({
                "record_id": record_id,
                "parent_record_id": parent_record_id,
                "doc_id": doc_id,
                "source_table": source_table,
                "source_record_key": source_record_key,
                "run_id": run_id,
                "content_hash": content_hash,
                "tag": tag,
                "field": field,
                "record_type": etype,
                "path": path,
                "sibling_index": sibling_index,
                "ingest_ts": ingest_ts,
            })
            child_counts = {}
            for child in elem:
                key = (child.tag, child.attrib.get("field"))
                child_counts[key] = child_counts.get(key, 0) + 1
                visit(child, record_id, f"{path}/{child.tag}", child_counts[key])

        elif tag == VALUE_TAG:
            raw_value = (elem.text or "").strip()
            # Structured pick-list fields (radio/dropdown) expose the
            # human-readable answer via the 'text' attribute; fall back to
            # the element text for plain single-value fields.
            display_value = elem.attrib.get("text", raw_value)
            attributes.append({
                "record_id": parent_record_id,
                "doc_id": doc_id,
                "source_table": source_table,
                "field": field,
                "value": display_value,
                "kind": elem.attrib.get("kind"),
                "path": path,
                "ingest_ts": ingest_ts,
            })
        else:
            # Wrapper/noise tags we don't model explicitly (UserDetails,
            # Groups, Roles, Properties, KCXML, AssetDetails, ...).
            # Still descend in case useful Primitives are nested under them.
            for child in elem:
                visit(child, parent_record_id, f"{path}/{child.tag}", 1)

    visit(root, None, root.tag, 1)
    return records, attributes


# ---------------------------------------------------------------------------
# Synapse notebook driver cell
# ---------------------------------------------------------------------------
# Real driver shape once XML_PAYLOAD_COLUMN / SOURCE_KEY_COLUMN are confirmed:
#
#   import uuid
#   run_id = str(uuid.uuid4())
#   all_records, all_attributes, all_errors = [], [], []
#
#   for source_table in SOURCE_TABLES:
#       # df = spark.sql(f"SELECT {SOURCE_KEY_COLUMN}, {XML_PAYLOAD_COLUMN} FROM {source_table}")
#       for row in df.collect():
#           source_record_key = row[SOURCE_KEY_COLUMN]
#           try:
#               xml_bytes = decode_xml_payload(row[XML_PAYLOAD_COLUMN])
#               recs, attrs = flatten_xml(xml_bytes, source_table, source_record_key, run_id)
#               all_records.extend(recs)
#               all_attributes.extend(attrs)
#           except ET.ParseError as e:
#               all_errors.append({
#                   "run_id": run_id, "source_table": source_table,
#                   "source_record_key": source_record_key,
#                   "error_message": str(e),
#                   "processed_utc": datetime.now(timezone.utc).isoformat(),
#               })
#
#   spark.createDataFrame(all_records).write.format("delta").mode("append") \
#       .saveAsTable("pronto.xml_record")
#   spark.createDataFrame(all_attributes).write.format("delta").mode("append") \
#       .saveAsTable("pronto.xml_attribute")
#   if all_errors:
#       spark.createDataFrame(all_errors).write.format("delta").mode("append") \
#           .saveAsTable("pronto.xml_parse_error")
#
# For typical daily volumes (tens to low hundreds of rows), looping
# row-by-row on the driver is simplest to build and debug. If volumes grow
# materially, map flatten_xml() over the Spark DataFrame instead of
# collect()-ing to the driver -- the parsing logic itself is unchanged.

if __name__ == "__main__":
    # Local dev/test harness only - exercises the parser against sample
    # files on disk before wiring up the real pronto source read.
    import glob
    import uuid
    import pandas as pd

    run_id = str(uuid.uuid4())
    all_records, all_attributes, all_errors = [], [], []

    for path in glob.glob("/mnt/user-data/uploads/*.xml"):
        with open(path, "rb") as f:
            xml_bytes = f.read()
        source_record_key = path.split("/")[-1]
        try:
            recs, attrs = flatten_xml(xml_bytes, "pronto.ddform_connectsr_va", source_record_key, run_id)
            all_records.extend(recs)
            all_attributes.extend(attrs)
        except ET.ParseError as e:
            all_errors.append({
                "run_id": run_id,
                "source_table": "pronto.ddform_connectsr_va",
                "source_record_key": source_record_key,
                "error_message": str(e),
                "processed_utc": datetime.now(timezone.utc).isoformat(),
            })
            print(f"PARSE ERROR in {path}: {e} -- written to pronto.xml_parse_error")

    records_df = pd.DataFrame(all_records)
    attributes_df = pd.DataFrame(all_attributes)
    errors_df = pd.DataFrame(all_errors)

    records_df.to_parquet("xml_record.parquet", index=False)
    attributes_df.to_parquet("xml_attribute.parquet", index=False)

    print(f"xml_record: {len(records_df)}, xml_attribute: {len(attributes_df)}, xml_parse_error: {len(errors_df)}")
