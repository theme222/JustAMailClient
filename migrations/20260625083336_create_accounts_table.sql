-- Add migration script here
CREATE TABLE IF NOT EXISTS accounts ( -- User accounts only
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    create_time INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    update_time INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000 AS INTEGER)),
    local_part TEXT NOT NULL, -- username
    domain TEXT NOT NULL,
    fetch_server TEXT NOT NULL,
    push_server TEXT NOT NULL,
    UNIQUE(local_part, domain)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_accounts_local_part ON accounts(local_part);
CREATE INDEX IF NOT EXISTS idx_accounts_domain ON accounts(domain);
