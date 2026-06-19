CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    local_part TEXT NOT NULL, -- username
    domain TEXT NOT NULL,
    type TEXT NOT NULL -- TBD
);

CREATE INDEX IF NOT EXISTS idx_accounts_local_part ON accounts(local_part);
CREATE INDEX IF NOT EXISTS idx_accounts_domain ON accounts(domain);
