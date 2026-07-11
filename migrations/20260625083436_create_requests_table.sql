-- Add migration script here
CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL, -- TBD
    args BLOB, -- [JSONB]
    request_time INTEGER NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_time INTEGER
) STRICT;

CREATE INDEX IF NOT EXISTS idx_requests_request_time ON requests(request_time);
CREATE INDEX IF NOT EXISTS idx_requests_finished_time ON requests(finished_time);