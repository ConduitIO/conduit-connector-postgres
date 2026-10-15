-- Temporal edge values: infinity, BC and five-digit years. Not part of the
-- conduit golden workload; captured with it for this connector (see the
-- README). The CREATE runs with 01-schema.sql, the INSERTs after
-- 02-stream.sql and before its sentinel insert, which is where the capture
-- ran them.
CREATE TABLE golden.temporal (
    id    integer PRIMARY KEY,
    ts    timestamp,
    ts3   timestamp(3),
    tstz  timestamptz,
    d     date
);

INSERT INTO golden.temporal VALUES (1, 'infinity', 'infinity', 'infinity', NULL);
INSERT INTO golden.temporal VALUES (2, '-infinity', '-infinity', '-infinity', NULL);
INSERT INTO golden.temporal VALUES (3, '0044-03-15 12:00:00.123456 BC', '0044-03-15 12:00:00.123 BC', '0044-03-15 12:00:00.123456+00 BC', '0044-03-15 BC');
INSERT INTO golden.temporal VALUES (4, '10000-01-01 00:00:00.5', '10000-01-01 00:00:00.5', '10000-01-01 00:00:00.5+00', '10000-01-01');
INSERT INTO golden.temporal VALUES (5, NULL, NULL, NULL, 'infinity');
INSERT INTO golden.temporal VALUES (6, NULL, NULL, NULL, '-infinity');
