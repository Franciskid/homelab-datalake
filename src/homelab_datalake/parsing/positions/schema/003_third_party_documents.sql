-- Documents about a person that someone else wrote (other parties, unions, associations: third-party support or
-- attack material in a candidate's folder of a public archive) or whose author cannot be established. They are
-- recorded for transparency and never count as the person's words: no text is kept, nothing is extracted from them.
CREATE TABLE third_party_documents (
  id          INTEGER PRIMARY KEY,
  person      TEXT NOT NULL REFERENCES people(id),   -- the person the document is filed under (supports / is about)
  source      TEXT NOT NULL,
  external_id TEXT NOT NULL,
  url         TEXT NOT NULL,
  title       TEXT,
  date        TEXT,
  publisher   TEXT,                                  -- organisation that issued it, as the archive records it
  opposes     TEXT,                                  -- who the document is against, when the archive says
  kind        TEXT NOT NULL CHECK (kind IN ('tiers', 'indetermine')),
  reason      TEXT,
  licence     TEXT,
  checked_at  TEXT NOT NULL,
  UNIQUE (source, external_id, person)
);
CREATE INDEX third_party_documents_person ON third_party_documents(person, source);
