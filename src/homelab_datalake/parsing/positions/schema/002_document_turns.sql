-- Speaker turns inside a document: the character spans (in the stored text file) that are the
-- person's own words. Extraction only reads these spans. context = agenda item / section title.
CREATE TABLE document_turns (
  id        INTEGER PRIMARY KEY,
  document  INTEGER NOT NULL REFERENCES documents(id),
  person    TEXT NOT NULL REFERENCES people(id),
  char_start INTEGER NOT NULL,
  char_end   INTEGER NOT NULL,
  context   TEXT,
  CHECK (char_end > char_start)
);
CREATE INDEX document_turns_document ON document_turns(document);
