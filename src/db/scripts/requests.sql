-- Table to store pending requests that are unstable (network reliant)
CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL, -- TBD
    args BLOB, -- [JSONB]
    request_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_time DATETIME
);

CREATE INDEX IF NOT EXISTS idx_requests_request_time ON requests(request_time);
CREATE INDEX IF NOT EXISTS idx_requests_finished_time ON requests(finished_time);