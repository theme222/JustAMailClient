-- Add migration script here
CREATE TABLE IF NOT EXISTS mailboxes (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    uid_validity INTEGER NOT NULL,
    highest_modseq INTEGER NOT NULL,
    flags BLOB NOT NULL, -- JSONB
    ty TEXT NOT NULL, -- TBD
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
) STRICT;

CREATE INDEX IF NOT EXISTS idx_mailboxes_account_id ON mailboxes (account_id);

CREATE TABLE IF NOT EXISTS mailboxes_messages ( -- Many mailboxes to Many messages
    mailbox_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    PRIMARY KEY (mailbox_id, message_id),
    FOREIGN KEY (mailbox_id) REFERENCES mailboxes(id) ON DELETE CASCADE,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
) STRICT;
