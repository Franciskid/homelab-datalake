-- Positions pipeline, schema version 1.
-- Applied by scripts/positions/db.py, which records the version in PRAGMA user_version
-- and in schema_migrations. Never edit a migration that has shipped: add 002_*.sql.

CREATE TABLE schema_migrations (
  version     INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  applied_at  TEXT NOT NULL
);

-- People we track. kind: candidate (2027 polls), orbit (party / allies, weighted), historical.
CREATE TABLE people (
  id            TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  kind          TEXT NOT NULL CHECK (kind IN ('candidate', 'orbit', 'historical')),
  party         TEXT,
  orbit_of      TEXT REFERENCES people(id),
  orbit_weight  REAL CHECK (orbit_weight IS NULL OR (orbit_weight > 0 AND orbit_weight <= 1)),
  priority      INTEGER,            -- 1..15 = the candidates with polling weight, then the rest
  birth_year    INTEGER,            -- disambiguates homonyms when matching chamber members
  tags          TEXT NOT NULL DEFAULT '[]',   -- JSON list, e.g. ["president"]
  CHECK ((kind = 'orbit') = (orbit_of IS NOT NULL))
);

-- How each source names a person: AN acteur uid, Sénat matricule, EP member id, vie-publique speaker name.
CREATE TABLE person_external_ids (
  person       TEXT NOT NULL REFERENCES people(id),
  source       TEXT NOT NULL CHECK (source IN ('an', 'senat', 'ep', 'vie-publique')),
  external_id  TEXT NOT NULL,
  external_name TEXT,
  method       TEXT NOT NULL,       -- exact-name | exact-name+birth-year | override
  matched_at   TEXT NOT NULL,
  PRIMARY KEY (source, external_id)
);
CREATE INDEX person_external_ids_person ON person_external_ids(person);

-- Every file fetched from the network (bulk dumps, pages, PDFs): provenance for everything else.
CREATE TABLE raw_files (
  id          INTEGER PRIMARY KEY,
  url         TEXT NOT NULL UNIQUE,
  path        TEXT NOT NULL,        -- relative to the repository root
  fetched_at  TEXT NOT NULL,
  sha256      TEXT NOT NULL,
  bytes       INTEGER NOT NULL,
  content_type TEXT,
  licence     TEXT
);

-- A text a person is responsible for. One row per (source item, person): a debate between two
-- tracked people gives two rows pointing at the same text file.
CREATE TABLE documents (
  id           INTEGER PRIMARY KEY,
  person       TEXT NOT NULL REFERENCES people(id),
  source_type  TEXT NOT NULL CHECK (source_type IN ('vote', 'speech', 'programme', 'press')),
  source       TEXT NOT NULL,       -- vie-publique | programme-manifest | ...
  external_id  TEXT NOT NULL,       -- id inside that source
  url          TEXT NOT NULL,
  title        TEXT,
  date         TEXT,                -- ISO date, or YYYY-MM / YYYY when that is all we know
  fetched_at   TEXT,
  sha256       TEXT,                -- of the extracted text file
  path         TEXT,                -- extracted plain text, relative to the repository root
  licence      TEXT,
  attribution  TEXT NOT NULL DEFAULT 'personnelle' CHECK (attribution IN ('personnelle', 'parti', 'douteuse')),
  status       TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'fetched', 'failed', 'skipped')),
  status_note  TEXT,
  chars        INTEGER,
  raw_file     INTEGER REFERENCES raw_files(id),
  UNIQUE (source, external_id, person)
);
CREATE INDEX documents_person ON documents(person, source_type);

-- What we looked for and did not find (e.g. a 2027 programme not yet published).
CREATE TABLE coverage_notes (
  person      TEXT NOT NULL REFERENCES people(id),
  source_type TEXT NOT NULL,
  period      TEXT NOT NULL,
  status      TEXT NOT NULL CHECK (status IN ('not-published', 'not-found', 'unavailable', 'partial', 'not-applicable')),
  note        TEXT,
  url         TEXT,
  checked_at  TEXT NOT NULL,
  PRIMARY KEY (person, source_type, period)
);

-- Roll-call ballots, stored once whatever the number of tracked voters.
CREATE TABLE ballots (
  id          TEXT PRIMARY KEY,     -- an-17-1234 | senat-2023-45 | ep-123456
  chamber     TEXT NOT NULL CHECK (chamber IN ('AN', 'Senat', 'PE')),
  term        TEXT,                 -- legislature / session year / EP term
  number      TEXT,
  date        TEXT NOT NULL,
  title       TEXT NOT NULL,
  object      TEXT,                 -- what is voted on (text, amendment, article)
  vote_type   TEXT,
  result      TEXT,
  n_for       INTEGER,
  n_against   INTEGER,
  n_abstain   INTEGER,
  url         TEXT,
  raw_file    INTEGER REFERENCES raw_files(id),
  extra       TEXT NOT NULL DEFAULT '{}'   -- JSON: procedure reference, requester, corrections
);
CREATE INDEX ballots_chamber_date ON ballots(chamber, date);

CREATE TABLE votes (
  ballot      TEXT NOT NULL REFERENCES ballots(id),
  person      TEXT NOT NULL REFERENCES people(id),
  position    TEXT NOT NULL CHECK (position IN ('pour', 'contre', 'abstention', 'non-votant')),
  group_ref   TEXT,
  by_proxy    INTEGER,
  PRIMARY KEY (ballot, person)
);
CREATE INDEX votes_person ON votes(person);

-- Each ballot is labelled once (topic + which side is "+"); every member's vote inherits it.
CREATE TABLE ballot_labels (
  id               INTEGER PRIMARY KEY,
  ballot           TEXT NOT NULL REFERENCES ballots(id),
  model            TEXT NOT NULL,   -- model name, or 'human:<initials>'
  codebook_version TEXT NOT NULL,
  topic            TEXT NOT NULL,
  plus_side        TEXT CHECK (plus_side IN ('pour', 'contre')),   -- NULL = no readable direction
  score            INTEGER CHECK (score BETWEEN -10 AND 10),        -- intensity of a "pour" vote
  justification    TEXT,
  run_id           TEXT NOT NULL,
  created_at       TEXT NOT NULL,
  raw              TEXT
);
CREATE INDEX ballot_labels_ballot ON ballot_labels(ballot);

-- One extraction pass over one document.
CREATE TABLE extraction_runs (
  id            INTEGER PRIMARY KEY,
  document      INTEGER NOT NULL REFERENCES documents(id),
  model         TEXT NOT NULL,
  extractor_version TEXT NOT NULL,
  started_at    TEXT NOT NULL,
  finished_at   TEXT,
  chunks        INTEGER,
  proposed      INTEGER,
  kept          INTEGER,
  dropped       INTEGER,
  seconds       REAL,
  status        TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'done', 'failed')),
  error         TEXT
);
CREATE INDEX extraction_runs_document ON extraction_runs(document);

CREATE TABLE statements (
  id          INTEGER PRIMARY KEY,
  document    INTEGER NOT NULL REFERENCES documents(id),
  person      TEXT NOT NULL REFERENCES people(id),
  date        TEXT,
  quote       TEXT NOT NULL,        -- exactly source_text[char_start:char_end]
  summary     TEXT,
  char_start  INTEGER NOT NULL,
  char_end    INTEGER NOT NULL,
  verified    INTEGER NOT NULL CHECK (verified IN (0, 1)),
  extraction_run INTEGER REFERENCES extraction_runs(id),
  model_quote TEXT,                 -- the quote as the model wrote it, when it differs
  UNIQUE (document, char_start, char_end)
);
CREATE INDEX statements_person ON statements(person);

-- Quotes proposed by the model and rejected by the verbatim check (kept for audit and counting).
CREATE TABLE extraction_drops (
  id          INTEGER PRIMARY KEY,
  extraction_run INTEGER NOT NULL REFERENCES extraction_runs(id),
  quote       TEXT,
  summary     TEXT,
  reason      TEXT NOT NULL         -- not-found | too-short | too-long | question | duplicate
);

CREATE TABLE labels (
  id               INTEGER PRIMARY KEY,
  statement        INTEGER NOT NULL REFERENCES statements(id),
  model            TEXT NOT NULL,
  codebook_version TEXT NOT NULL,
  topic            TEXT NOT NULL,
  score            INTEGER CHECK (score BETWEEN -10 AND 10),
  justification    TEXT,
  run_id           TEXT NOT NULL,
  created_at       TEXT NOT NULL,
  prompt_hash      TEXT,
  raw              TEXT             -- JSON of both steps' raw outputs
);
CREATE INDEX labels_statement ON labels(statement);

-- Human decisions from the review tool. Exactly one target: a statement or a ballot.
CREATE TABLE reviews (
  id          INTEGER PRIMARY KEY,
  statement   INTEGER REFERENCES statements(id),
  ballot      TEXT REFERENCES ballots(id),
  reviewer    TEXT NOT NULL,
  decision    TEXT NOT NULL CHECK (decision IN ('accept', 'correct', 'reject')),
  topic       TEXT,
  score       INTEGER CHECK (score BETWEEN -10 AND 10),
  plus_side   TEXT CHECK (plus_side IN ('pour', 'contre')),
  note        TEXT,
  codebook_version TEXT,
  created_at  TEXT NOT NULL,
  CHECK ((statement IS NULL) != (ballot IS NULL))
);
