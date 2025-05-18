-- IRS 990 Dynamic Schema
-- Version: 2.0
-- Date: 2025-05-17

-- Organizations table
CREATE TABLE IF NOT EXISTS organizations (
    ein VARCHAR(9) PRIMARY KEY,
    name TEXT,
    city TEXT,
    state TEXT
);

-- Filings table
CREATE TABLE IF NOT EXISTS filings (
    filing_id VARCHAR(30) PRIMARY KEY,
    ein VARCHAR(9) REFERENCES organizations(ein),
    form_type VARCHAR(10),
    tax_period VARCHAR(6),
    submission_date DATE,
    tax_year INTEGER
);

-- Field definitions table
CREATE TABLE IF NOT EXISTS field_definitions (
    field_id SERIAL PRIMARY KEY,
    xpath TEXT NOT NULL,
    name TEXT,
    description TEXT,
    form_type VARCHAR(10),
    field_type VARCHAR(20)
);
CREATE INDEX IF NOT EXISTS idx_field_definitions_xpath ON field_definitions(xpath);

-- Filing values table (EAV model)
CREATE TABLE IF NOT EXISTS filing_values (
    filing_id VARCHAR(30) REFERENCES filings(filing_id),
    field_id INTEGER REFERENCES field_definitions(field_id),
    value TEXT,
    PRIMARY KEY (filing_id, field_id)
);
CREATE INDEX IF NOT EXISTS idx_filing_values_filing_id ON filing_values(filing_id);

-- Repeating groups table
CREATE TABLE IF NOT EXISTS repeating_groups (
    group_id SERIAL PRIMARY KEY,
    filing_id VARCHAR(30) REFERENCES filings(filing_id),
    parent_group_id INTEGER REFERENCES repeating_groups(group_id),
    name TEXT,
    xpath TEXT
);
CREATE INDEX IF NOT EXISTS idx_repeating_groups_filing_id ON repeating_groups(filing_id);

-- Repeating group values table
CREATE TABLE IF NOT EXISTS repeating_group_values (
    group_id INTEGER REFERENCES repeating_groups(group_id),
    field_id INTEGER REFERENCES field_definitions(field_id),
    value TEXT,
    PRIMARY KEY (group_id, field_id)
);
CREATE INDEX IF NOT EXISTS idx_repeating_group_values_group_id ON repeating_group_values(group_id);

-- Performance indexes for common operations
CREATE INDEX IF NOT EXISTS idx_organizations_ein ON organizations(ein);
CREATE INDEX IF NOT EXISTS idx_filings_ein ON filings(ein);
CREATE INDEX IF NOT EXISTS idx_filings_tax_year ON filings(tax_year);
CREATE INDEX IF NOT EXISTS idx_filings_form_type ON filings(form_type);
