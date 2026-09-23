ALTER TABLE login_code_message_deliveries
    ADD COLUMN template text NOT NULL DEFAULT '';

COMMENT ON COLUMN login_code_message_deliveries.template IS
    'Rendered first-delivery template with {{code}} placeholder, never the secret code; empty means legacy built-in template';
