-- A second database whose server_encoding is SQL_ASCII. Postgres stores bytes
-- unchecked in that encoding, so a json value can hold text that is not
-- valid UTF-8; the json/jsonb tests use it to check the open-time warning,
-- the base64 delivery and the halt (json.invalidUTF8,
-- postgres.source.json_not_utf8). UTF8 databases reject such text on input,
-- so it cannot be produced in meroxadb.
CREATE DATABASE meroxadb_sqlascii
    WITH ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0
    OWNER meroxauser;
