-- BotFather now answers /deletebot and its /help is sectioned like the official
-- bot. Keep the published command menu in sync: /deletebot is discoverable from
-- the '/' menu, and the descriptions of the pre-existing entries use the
-- official wording instead of the old local phrasing.
UPDATE public.bots
SET commands = (
        SELECT COALESCE(jsonb_agg(
                 CASE item.command->>'command'
                   WHEN 'mybots' THEN jsonb_set(item.command, '{description}', '"edit your bots"'::jsonb)
                   WHEN 'token' THEN jsonb_set(item.command, '{description}', '"get authorization token"'::jsonb)
                   WHEN 'revoke' THEN jsonb_set(item.command, '{description}', '"revoke bot access token"'::jsonb)
                   ELSE item.command
                 END ORDER BY item.ordinal), '[]'::jsonb)
        FROM jsonb_array_elements(commands) WITH ORDINALITY AS item(command, ordinal)
      ),
    updated_at = now()
WHERE bot_user_id = 93372553;

UPDATE public.bots
SET commands = commands || '[
      {"command":"deletebot","description":"delete a bot"}
    ]'::jsonb,
    updated_at = now()
WHERE bot_user_id = 93372553
  AND NOT EXISTS (
      SELECT 1
      FROM jsonb_array_elements(commands) AS item(command)
      WHERE item.command->>'command' = 'deletebot'
  );

-- Command menus are cached by bot_info_version, so bump it to force connected
-- clients to refresh the authoritative list.
UPDATE public.users
SET bot_info_version = bot_info_version + 1,
    updated_at = now()
WHERE id = 93372553;