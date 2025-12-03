
DROP TABLE IF EXISTS region_counts CASCADE;
DROP TABLE IF EXISTS ingest_log CASCADE;
DROP TABLE IF EXISTS units CASCADE;
DROP TABLE IF EXISTS scrna_cluster_markers CASCADE;
DROP TABLE IF EXISTS scrna_clusters CASCADE;
DROP TABLE IF EXISTS scrna_samples CASCADE;
DROP TABLE IF EXISTS microscopy_files CASCADE;
DROP TABLE IF EXISTS sessions CASCADE;
DROP TABLE IF EXISTS brain_regions CASCADE;
DROP TABLE IF EXISTS subjects CASCADE;
DROP SCHEMA IF EXISTS rna CASCADE;

CREATE TABLE subjects (
    subject_id VARCHAR(50) PRIMARY KEY, -- e.g., 'sub-dbl01'
    original_id VARCHAR(100) UNIQUE,    -- e.g., 'DBL_A'
    sex CHAR(1) NOT NULL CHECK (sex IN ('M','F','U')),
    experiment_type VARCHAR(50) NOT NULL CHECK (experiment_type IN ('double_injection','rabies')),
    details TEXT
);

-- 2. BRAIN REGIONS (Dictionary)
CREATE TABLE brain_regions (
    region_id INT PRIMARY KEY,          -- Matches Allen Brain Atlas ID
    name VARCHAR(255) NOT NULL,         -- Region name
    acronym VARCHAR(50) NOT NULL,
    parent_id INT REFERENCES brain_regions(region_id),
    st_level INT,
    atlas_id INT,
    ontology_id INT
);

-- 2b. Sessions (for imaging/omics runs)
CREATE TABLE sessions (
    session_id VARCHAR(50) PRIMARY KEY,
    subject_id VARCHAR(50) REFERENCES subjects(subject_id),
    modality VARCHAR(50) NOT NULL, -- e.g., rabies, double_injection, scrna
    session_date DATE,
    protocol TEXT,
    notes TEXT
);

-- 2c. Microscopy files (BIDS-like runs)
CREATE TABLE microscopy_files (
    file_id SERIAL PRIMARY KEY,
    session_id VARCHAR(50) REFERENCES sessions(session_id),
    run INT,
    hemisphere VARCHAR(20) CHECK (hemisphere IN ('left','right','bilateral')),
    path TEXT NOT NULL,
    sha256 CHAR(64),
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(session_id, run, hemisphere)
);

-- 4. Units lookup (for FAIR metadata) - defined before region_counts to satisfy FKs
CREATE TABLE units (
    unit_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE, -- e.g., pixels, mm2, count
    description TEXT
);

-- 3. EXPERIMENTAL DATA (The Metrics)
CREATE TABLE region_counts (
    id SERIAL PRIMARY KEY,
    subject_id VARCHAR(50) REFERENCES subjects(subject_id),
    region_id INT REFERENCES brain_regions(region_id),
    file_id INT REFERENCES microscopy_files(file_id),
    
    -- Metrics from your CSV
    region_pixels BIGINT NOT NULL,
    region_area_mm FLOAT,      -- Mapped from 'Region area'
    object_count INT,          -- Handles 'N/A' by storing NULL
    object_pixels BIGINT,
    object_area_mm FLOAT,
    load FLOAT NOT NULL,
    norm_load FLOAT,
    
    -- Metadata
    hemisphere VARCHAR(20) NOT NULL CHECK (hemisphere IN ('left','right','bilateral')),
    region_pixels_unit_id INT REFERENCES units(unit_id),
    region_area_unit_id INT REFERENCES units(unit_id),
    object_count_unit_id INT REFERENCES units(unit_id),
    object_pixels_unit_id INT REFERENCES units(unit_id),
    object_area_unit_id INT REFERENCES units(unit_id),
    load_unit_id INT REFERENCES units(unit_id),

    CONSTRAINT region_counts_uniq UNIQUE (subject_id, region_id, hemisphere)
);

-- 5. Ingest log for provenance
CREATE TABLE ingest_log (
    ingest_id SERIAL PRIMARY KEY,
    source_path TEXT,
    checksum CHAR(64),
    rows_loaded INT,
    status VARCHAR(20),
    message TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- 6. scRNA-seq schema
CREATE TABLE scrna_samples (
    sample_id VARCHAR(50) PRIMARY KEY,
    subject_id VARCHAR(50) REFERENCES subjects(subject_id),
    tissue VARCHAR(100),
    prep VARCHAR(100),
    date_run DATE,
    protocol TEXT
);

CREATE TABLE scrna_clusters (
    sample_id VARCHAR(50) REFERENCES scrna_samples(sample_id),
    cluster_id VARCHAR(50),
    n_cells INT,
    description TEXT,
    PRIMARY KEY (sample_id, cluster_id)
);

CREATE TABLE scrna_cluster_markers (
    sample_id VARCHAR(50),
    cluster_id VARCHAR(50),
    gene VARCHAR(100),
    logfc FLOAT,
    pval_adj FLOAT,
    PRIMARY KEY (sample_id, cluster_id, gene),
    FOREIGN KEY (sample_id, cluster_id) REFERENCES scrna_clusters(sample_id, cluster_id)
);

-- Indexes
CREATE INDEX idx_region_counts_subject ON region_counts(subject_id);
CREATE INDEX idx_region_counts_region ON region_counts(region_id);
CREATE INDEX idx_region_counts_hemi ON region_counts(hemisphere);
CREATE INDEX idx_brain_regions_parent ON brain_regions(parent_id);
CREATE INDEX idx_microscopy_files_session ON microscopy_files(session_id);
CREATE INDEX idx_scrna_clusters_sample ON scrna_clusters(sample_id);

-- RNA schema (kept independent from imaging data)
CREATE SCHEMA rna;

-- Dataset-level metadata and matrix source (e.g., WMB-10Xv2-OLF-log2.h5ad)
CREATE TABLE rna.datasets (
    dataset_id SERIAL PRIMARY KEY,
    label VARCHAR(100) UNIQUE NOT NULL,            -- e.g., 'WMB-10Xv2-OLF'
    modality VARCHAR(30) NOT NULL CHECK (modality IN ('rna_seq','merfish')),
    normalization VARCHAR(100),                    -- e.g., 'log2(CPM+1)'
    matrix_path TEXT NOT NULL,                     -- path to h5ad/csv
    matrix_format VARCHAR(20) NOT NULL DEFAULT 'h5ad',
    is_log_transformed BOOLEAN DEFAULT FALSE,
    n_cells INT,
    n_genes INT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Library labels from obs.library_label categories
CREATE TABLE rna.libraries (
    dataset_id INT REFERENCES rna.datasets(dataset_id) ON DELETE CASCADE,
    library_label VARCHAR(120) NOT NULL,
    PRIMARY KEY (dataset_id, library_label)
);

-- Gene dictionary scoped to dataset
CREATE TABLE rna.genes (
    gene_id SERIAL PRIMARY KEY,
    dataset_id INT NOT NULL REFERENCES rna.datasets(dataset_id) ON DELETE CASCADE,
    gene_identifier VARCHAR(64),   -- e.g., Ensembl ID
    gene_symbol VARCHAR(64),
    CONSTRAINT rna_genes_uniq UNIQUE (dataset_id, gene_identifier)
);

-- Clusters (cluster_alias + label + cell counts)
CREATE TABLE rna.clusters (
    dataset_id INT REFERENCES rna.datasets(dataset_id) ON DELETE CASCADE,
    cluster_alias INT NOT NULL,
    cluster_label VARCHAR(120),
    number_of_cells INT,
    PRIMARY KEY (dataset_id, cluster_alias)
);

-- Cluster annotation sets (term_set)
CREATE TABLE rna.cluster_annotation_term_sets (
    dataset_id INT NOT NULL REFERENCES rna.datasets(dataset_id) ON DELETE CASCADE,
    term_set_label VARCHAR(120) NOT NULL, -- e.g., CCN20230722_NEUR
    name VARCHAR(255),
    description TEXT,
    term_set_order INT,
    PRIMARY KEY (dataset_id, term_set_label)
);

-- Cluster annotation terms
CREATE TABLE rna.cluster_annotation_terms (
    dataset_id INT NOT NULL REFERENCES rna.datasets(dataset_id) ON DELETE CASCADE,
    term_label VARCHAR(120) NOT NULL, -- e.g., CS20230722_NEUR_Glut
    name VARCHAR(255) NOT NULL,
    term_set_label VARCHAR(120) NOT NULL,
    parent_term_label VARCHAR(120),
    parent_term_set_label VARCHAR(120),
    term_set_order INT,
    term_order INT,
    cluster_annotation_term_set_name VARCHAR(255),
    color_hex_triplet CHAR(7),
    PRIMARY KEY (dataset_id, term_label),
    CONSTRAINT fk_rna_term_set FOREIGN KEY (dataset_id, term_set_label) REFERENCES rna.cluster_annotation_term_sets(dataset_id, term_set_label) ON DELETE CASCADE,
    CONSTRAINT fk_rna_parent_term FOREIGN KEY (dataset_id, parent_term_label) REFERENCES rna.cluster_annotation_terms(dataset_id, term_label) ON DELETE SET NULL
);

-- Mapping clusters to annotation terms
CREATE TABLE rna.cluster_annotation_membership (
    dataset_id INT NOT NULL REFERENCES rna.datasets(dataset_id) ON DELETE CASCADE,
    cluster_alias INT NOT NULL,
    term_label VARCHAR(120) NOT NULL,
    term_set_label VARCHAR(120) NOT NULL,
    cluster_annotation_term_name VARCHAR(255),
    cluster_annotation_term_set_name VARCHAR(255),
    number_of_cells INT,
    color_hex_triplet CHAR(7),
    PRIMARY KEY (dataset_id, cluster_alias, term_label),
    CONSTRAINT fk_rna_member_cluster FOREIGN KEY (dataset_id, cluster_alias) REFERENCES rna.clusters(dataset_id, cluster_alias) ON DELETE CASCADE,
    CONSTRAINT fk_rna_member_term FOREIGN KEY (dataset_id, term_label) REFERENCES rna.cluster_annotation_terms(dataset_id, term_label) ON DELETE CASCADE,
    CONSTRAINT fk_rna_member_term_set FOREIGN KEY (dataset_id, term_set_label) REFERENCES rna.cluster_annotation_term_sets(dataset_id, term_set_label) ON DELETE SET NULL
);

-- Cell metadata (MERFISH/10X)
CREATE TABLE rna.cells (
    cell_label VARCHAR(120) PRIMARY KEY, -- e.g., '1019171907102340387-1'
    dataset_id INT NOT NULL REFERENCES rna.datasets(dataset_id) ON DELETE CASCADE,
    cell_barcode VARCHAR(50),
    library_label VARCHAR(120),
    cluster_alias INT,
    anatomical_division_label VARCHAR(50),
    brain_section_label VARCHAR(120),
    average_correlation_score DOUBLE PRECISION,
    feature_matrix_label VARCHAR(120),
    donor_label VARCHAR(120),
    donor_genotype VARCHAR(120),
    donor_sex CHAR(1),
    x DOUBLE PRECISION,
    y DOUBLE PRECISION,
    z DOUBLE PRECISION,
    neurotransmitter VARCHAR(50),
    cell_class VARCHAR(50),
    cell_subclass VARCHAR(50),
    cell_supertype VARCHAR(50),
    cell_cluster_label VARCHAR(120),
    neurotransmitter_color CHAR(7),
    cell_class_color CHAR(7),
    cell_subclass_color CHAR(7),
    cell_supertype_color CHAR(7),
    cell_cluster_color CHAR(7),
    group_hy_ea_glut_gaba BOOLEAN,
    group_mb_hb_cb_gaba BOOLEAN,
    group_mb_hb_glut_sero_dopa BOOLEAN,
    group_nn_imn_gc BOOLEAN,
    group_pallium_glut BOOLEAN,
    group_subpallium_gaba BOOLEAN,
    group_th_epi_glut BOOLEAN,
    group_wholebrain BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT fk_rna_cells_cluster FOREIGN KEY (dataset_id, cluster_alias) REFERENCES rna.clusters(dataset_id, cluster_alias) ON DELETE SET NULL,
    CONSTRAINT fk_rna_cells_library FOREIGN KEY (dataset_id, library_label) REFERENCES rna.libraries(dataset_id, library_label) ON DELETE SET NULL
);

-- Provenance for RNA ingest
CREATE TABLE rna.ingest_log (
    ingest_id SERIAL PRIMARY KEY,
    dataset_id INT REFERENCES rna.datasets(dataset_id) ON DELETE CASCADE,
    source_path TEXT,
    checksum CHAR(64),
    rows_loaded INT,
    status VARCHAR(20),
    message TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- RNA indexes
CREATE INDEX idx_rna_genes_symbol ON rna.genes(dataset_id, gene_symbol);
CREATE INDEX idx_rna_cells_dataset ON rna.cells(dataset_id);
CREATE INDEX idx_rna_cells_cluster ON rna.cells(dataset_id, cluster_alias);
CREATE INDEX idx_rna_cells_library ON rna.cells(dataset_id, library_label);
CREATE INDEX idx_rna_clusters_dataset ON rna.clusters(dataset_id);
CREATE INDEX idx_rna_cluster_member_term ON rna.cluster_annotation_membership(dataset_id, term_label);
CREATE INDEX idx_rna_ingest_dataset ON rna.ingest_log(dataset_id);
