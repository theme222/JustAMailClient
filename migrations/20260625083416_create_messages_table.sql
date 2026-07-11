-- Add migration script here
CREATE TABLE IF NOT EXISTS messages (
    /* Internal Fields */
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- Unique message id number 5,937,510
    account_id INTEGER NOT NULL, -- id of the account this message was sent from / recieved from
    ty TEXT, -- TBD
    last_sync_time INTEGER NOT NULL,
    last_query_time INTEGER,
    /* Internal Fields */
    /* Other Fields */
    flags BLOB NOT NULL, -- JSONB
    size INTEGER NOT NULL, -- CONST
    internal_date INTEGER NOT NULL, -- CONST time in milliseconds since epoch
    bodystructure BLOB NOT NULL, -- CONST JSONB
    /* Other Fields */
    /* Header Fields */
    imap_uid INTEGER, -- Standard IMAP UID
    modseq INTEGER,
    -- gmail_msg_id INTEGER, -- X-GM-MSGID (64-bit unsigned int)
    -- gmail_thread_id INTEGER, -- X-GM-THRID (64-bit unsigned int)
    /* Envelope Fields */
    rfc_message_id TEXT, -- CONST Standard Message-ID header
    env_date TEXT, -- CONST
    env_subject TEXT, -- CONST
    env_from BLOB, -- CONST
    env_reply_to BLOB, -- CONST
    env_to BLOB, -- CONST JSONB
    env_cc BLOB, -- CONST JSONB
    env_bcc BLOB, -- CONST JSONB
    env_in_reply_to TEXT, -- CONST
    /* Envelope Fields */
    header_raw BLOB, 
    /* Header Fields */
    /* Body Fields */
    body_preview TEXT NOT NULL, -- CONST First 8192 bytes of the body fully parsed and extracted only useful content (used for previews and searching)
    /* Body Fields */
    FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE
    -- CANDIDATE KEY (account_id, imap_uid, mailbox.uid_validity)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_messages_account_id ON messages(account_id);
CREATE INDEX IF NOT EXISTS idx_messages_imap_uid ON messages(imap_uid);
-- CREATE INDEX IF NOT EXISTS idx_messages_gmail_msg_id ON messages(gmail_msg_id);
-- CREATE INDEX IF NOT EXISTS idx_messages_gmail_thread_id ON messages(gmail_thread_id);
CREATE INDEX IF NOT EXISTS idx_messages_internal_date ON messages(internal_date);

CREATE VIRTUAL TABLE message_search USING fts5(
    env_subject, 
    body_preview, 
    content='messages', 
    content_rowid='id',
    tokenize='unicode61 remove_diacritics 2',
    detail=column
);

-- Trigger Warning 
CREATE TRIGGER trg_messages_ai AFTER INSERT ON messages BEGIN
    INSERT INTO message_search(rowid, env_subject, body_preview) 
    VALUES (new.id, new.env_subject, new.body_preview);
END;

CREATE TRIGGER trg_messages_ad AFTER DELETE ON messages BEGIN
    INSERT INTO message_search(message_search, rowid, env_subject, body_preview) 
    VALUES ('delete', old.id, old.env_subject, old.body_preview);
END;

CREATE TRIGGER trg_messages_au AFTER UPDATE ON messages BEGIN
    INSERT INTO message_search(message_search, rowid, env_subject, body_preview) 
    VALUES ('delete', old.id, old.env_subject, old.body_preview);
    INSERT INTO message_search(rowid, env_subject, body_preview) 
    VALUES (new.id, new.env_subject, new.body_preview);
END;

CREATE TABLE message_parts ( -- Only contains data on leaf node sections of the bodystructure
    message_id INTEGER NOT NULL, 
    part_spec TEXT NOT NULL, -- RFC 822 part ID
    data BLOB,
    PRIMARY KEY (message_id, part_spec),
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
) STRICT;