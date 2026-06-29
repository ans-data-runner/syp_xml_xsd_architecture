/* ==========================================================================
   SYP Connect VA/VC - Serverless SQL layer
   Pool: synw-symdp-data-d-uksouth-001   Lakehouse: synw-syp-lakehouse

   Reads the generic processed objects written by the notebook into the
   pronto schema (pronto.xml_record / pronto.xml_attribute / pronto.xml_parse_error,
   alongside the existing pronto.ddform_connectsr_va / pronto.ddform_connectsr_vc
   source tables) and exposes:
     - pronto_vw  : validation / review views (engineering + SME use)
     - pbi_vw     : Power BI reporting views (stable business columns only)

   NOTE: the examples below assume pronto.xml_record / xml_attribute /
   xml_parse_error are managed/external tables the notebook writes directly.
   If instead the notebook writes Parquet files surfaced via OPENROWSET,
   swap the FROM clauses for OPENROWSET(...) over the relevant lakehouse
   path - confirm which pattern this platform already uses for other pronto
   outputs (see design doc, Open Questions).
   ========================================================================== */

CREATE SCHEMA pronto_vw;
GO
CREATE SCHEMA pbi_vw;
GO

/* --------------------------------------------------------------------------
   1. pronto_vw - validation / review
   -------------------------------------------------------------------------- */

CREATE OR ALTER VIEW pronto_vw.vw_xml_parse_errors AS
SELECT
    run_id,
    source_table,
    source_record_key,
    error_message,
    processed_utc
FROM pronto.xml_parse_error;
GO

CREATE OR ALTER VIEW pronto_vw.vw_xml_field_coverage AS
SELECT
    source_table,
    field,
    COUNT_BIG(*)                  AS field_count,
    COUNT_BIG(DISTINCT doc_id)    AS document_count
FROM pronto.xml_attribute
GROUP BY source_table, field;
GO

CREATE OR ALTER VIEW pronto_vw.vw_xml_sample_primitives AS
SELECT TOP (1000)
    source_table,
    doc_id,
    record_id,
    path,
    field,
    value,
    kind,
    ingest_ts
FROM pronto.xml_attribute
ORDER BY ingest_ts DESC;
GO

-- Reconciliation check: source rows vs parsed + errored rows
CREATE OR ALTER VIEW pronto_vw.vw_xml_run_reconciliation AS
SELECT
    r.run_id,
    r.source_table,
    COUNT(DISTINCT r.doc_id)                                   AS documents_parsed,
    (SELECT COUNT(*) FROM pronto.xml_parse_error e
       WHERE e.run_id = r.run_id AND e.source_table = r.source_table) AS documents_errored
FROM pronto.xml_record r
GROUP BY r.run_id, r.source_table;
GO

/* --------------------------------------------------------------------------
   2. pbi_vw - Power BI reporting
      Pattern: filter pronto.xml_record to the record 'field' that represents
      the entity, join pronto.xml_attribute on record_id, pivot known fields.
      Adding a new field to capture later = add one line here, no notebook
      change required.
   -------------------------------------------------------------------------- */

-- Incident (VA): the top-level CompoundRecord that carries IncidentDate etc.
CREATE OR ALTER VIEW pbi_vw.vw_va_incident AS
SELECT
    r.doc_id                                                    AS incident_id,
    MAX(CASE WHEN a.field = 'IncidentDate' THEN a.value END)    AS incident_date,
    MAX(CASE WHEN a.field = 'StormURN' THEN a.value END)        AS storm_urn,
    MAX(CASE WHEN a.field = 'IncidentForce' THEN a.value END)   AS force,
    MAX(CASE WHEN a.field = 'PrimaryOfficer' THEN a.value END)  AS primary_officer_flag,
    MAX(CASE WHEN a.field = 'EnteredBy' THEN a.value END)       AS entered_by,
    MAX(r.ingest_ts)                                            AS ingest_ts
FROM pronto.xml_record r
JOIN pronto.xml_attribute a ON a.record_id = r.record_id
WHERE r.tag = 'CompoundRecord'
  AND r.source_table = 'pronto.ddform_connectsr_va'
GROUP BY r.doc_id
HAVING MAX(CASE WHEN a.field = 'IncidentDate' THEN 1 ELSE 0 END) = 1;
GO

-- Incident (VC): same shape, VC source table
CREATE OR ALTER VIEW pbi_vw.vw_vc_incident AS
SELECT
    r.doc_id                                                    AS incident_id,
    MAX(CASE WHEN a.field = 'IncidentDate' THEN a.value END)    AS incident_date,
    MAX(CASE WHEN a.field = 'StormURN' THEN a.value END)        AS storm_urn,
    MAX(CASE WHEN a.field = 'IncidentForce' THEN a.value END)   AS force,
    MAX(r.ingest_ts)                                            AS ingest_ts
FROM pronto.xml_record r
JOIN pronto.xml_attribute a ON a.record_id = r.record_id
WHERE r.tag = 'CompoundRecord'
  AND r.source_table = 'pronto.ddform_connectsr_vc'
GROUP BY r.doc_id
HAVING MAX(CASE WHEN a.field = 'IncidentDate' THEN 1 ELSE 0 END) = 1;
GO

-- Person (VA): CompoundRecord with field = 'Person', detail held one level
-- down in a SimpleRecord (field = 'Details').
CREATE OR ALTER VIEW pbi_vw.vw_va_person AS
SELECT
    p.record_id                                                        AS person_record_id,
    p.doc_id                                                           AS incident_id,
    MAX(CASE WHEN a.field = 'ConnectPersonGroupRef' THEN a.value END)  AS person_id,
    MAX(CASE WHEN a.field = 'Forename' THEN a.value END)               AS forename,
    MAX(CASE WHEN a.field = 'Surname' THEN a.value END)                AS surname,
    MAX(CASE WHEN a.field = 'DateOfBirth' THEN a.value END)            AS date_of_birth,
    MAX(CASE WHEN a.field = 'Sex' THEN a.value END)                    AS sex,
    MAX(CASE WHEN a.field = 'SelfDefinedEthnicity' THEN a.value END)   AS ethnicity,
    MAX(CASE WHEN a.field = 'Occupation' THEN a.value END)             AS occupation,
    MAX(CASE WHEN a.field = 'Nationality' THEN a.value END)            AS nationality
FROM pronto.xml_record p
JOIN pronto.xml_record child ON child.parent_record_id = p.record_id
JOIN pronto.xml_attribute a  ON a.record_id = child.record_id
WHERE p.field = 'Person' AND p.source_table = 'pronto.ddform_connectsr_va'
GROUP BY p.record_id, p.doc_id;
GO

-- Person (VC): same shape, VC source table
CREATE OR ALTER VIEW pbi_vw.vw_vc_person AS
SELECT
    p.record_id                                                        AS person_record_id,
    p.doc_id                                                           AS incident_id,
    MAX(CASE WHEN a.field = 'ConnectPersonGroupRef' THEN a.value END)  AS person_id,
    MAX(CASE WHEN a.field = 'Forename' THEN a.value END)               AS forename,
    MAX(CASE WHEN a.field = 'Surname' THEN a.value END)                AS surname,
    MAX(CASE WHEN a.field = 'DateOfBirth' THEN a.value END)            AS date_of_birth,
    MAX(CASE WHEN a.field = 'Sex' THEN a.value END)                    AS sex
FROM pronto.xml_record p
JOIN pronto.xml_record child ON child.parent_record_id = p.record_id
JOIN pronto.xml_attribute a  ON a.record_id = child.record_id
WHERE p.field = 'Person' AND p.source_table = 'pronto.ddform_connectsr_vc'
GROUP BY p.record_id, p.doc_id;
GO

-- Person role: derived from the Involvement field alongside each Person
-- block. CONFIRM exact field name against a sample with multiple people on
-- one incident before relying on this in Power BI (see design doc, Open
-- Questions).
CREATE OR ALTER VIEW pbi_vw.vw_person_role AS
SELECT
    p.doc_id                                                            AS incident_id,
    MAX(CASE WHEN a.field = 'ConnectPersonGroupRef' THEN a.value END)   AS person_id,
    MAX(CASE WHEN a.field = 'Involvement' THEN a.value END)             AS role,
    p.source_table
FROM pronto.xml_record p
JOIN pronto.xml_record child ON child.parent_record_id = p.record_id
JOIN pronto.xml_attribute a  ON a.record_id = child.record_id
WHERE p.field = 'Person'
GROUP BY p.record_id, p.doc_id, p.source_table;
GO

-- Location
CREATE OR ALTER VIEW pbi_vw.vw_incident_location AS
SELECT
    l.record_id                                                AS location_record_id,
    l.doc_id                                                   AS incident_id,
    MAX(CASE WHEN a.field = 'Postcode' THEN a.value END)       AS postcode,
    MAX(CASE WHEN a.field = 'PremisesNumber' THEN a.value END) AS premises_no,
    MAX(CASE WHEN a.field = 'AddressLine1' THEN a.value END)   AS address_line1,
    MAX(CASE WHEN a.field = 'Town' THEN a.value END)           AS town,
    MAX(CASE WHEN a.field = 'DwellingType' THEN a.value END)   AS dwelling_type,
    l.source_table
FROM pronto.xml_record l
JOIN pronto.xml_attribute a ON a.record_id = l.record_id
WHERE l.field = 'Location'
GROUP BY l.record_id, l.doc_id, l.source_table;
GO

-- Risk assessment (VAQuestions block) - covers both VA and VC
CREATE OR ALTER VIEW pbi_vw.vw_risk_assessment AS
SELECT
    ra.doc_id                                                       AS incident_id,
    MAX(CASE WHEN a.field = 'SelfHarm' THEN a.value END)            AS self_harm,
    MAX(CASE WHEN a.field = 'SociallyIsolated' THEN a.value END)    AS socially_isolated,
    MAX(CASE WHEN a.field = 'TypeOfHarm' THEN a.value END)          AS harm_type,
    MAX(CASE WHEN a.field = 'MoreVulnerable' THEN a.value END)      AS vulnerability_flag,
    ra.source_table
FROM pronto.xml_record ra
JOIN pronto.xml_attribute a ON a.record_id = ra.record_id
WHERE ra.field = 'VAQuestions'
GROUP BY ra.record_id, ra.doc_id, ra.source_table;
GO

/* --------------------------------------------------------------------------
   3. Optional: materialise pbi_vw views as physical Parquet via CETAS for
      Power BI Import-mode performance at higher volumes.
   -------------------------------------------------------------------------- */
-- CREATE EXTERNAL TABLE pronto.gold_va_person
-- WITH (LOCATION = 'gold/va_person/', DATA_SOURCE = <lakehouse_data_source>, FILE_FORMAT = <parquet_format>)
-- AS SELECT * FROM pbi_vw.vw_va_person;
