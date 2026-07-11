-- Add migration script here
CREATE TABLE IF NOT EXISTS accounts ( -- User accounts only
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    local_part TEXT NOT NULL, -- username
    domain TEXT NOT NULL,
    ty TEXT, -- TBD
    fetch_server TEXT NOT NULL,
    push_server TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_accounts_local_part ON accounts(local_part);
CREATE INDEX IF NOT EXISTS idx_accounts_domain ON accounts(domain);
