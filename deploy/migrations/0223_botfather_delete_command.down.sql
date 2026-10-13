UPDATE public.bots
SET commands = (
        SELECT COALESCE(jsonb_agg(
                 CASE item.command->>'command'
                   WHEN 'mybots' THEN jsonb_set(item.command, '{description}', '"list your bots"'::jsonb)
                   WHEN 'token' THEN jsonb_set(item.command, '{description}', '"show a bot''s token"'::jsonb)
                   WHEN 'revoke' THEN jsonb_set(item.command, '{description}', '"revoke a bot''s token"'::jsonb)
                   ELSE item.command
                 END ORDER BY item.ordinal), '[]'::jsonb)
        FROM jsonb_array_elements(commands) WITH ORDINALITY AS item(command, ordinal)
      ),
    updated_at = now()
WHERE bot_user_id = 93372553;

UPDATE public.bots
SET commands = COALESCE((
      SELECT jsonb_agg(command ORDER BY ordinal)
      FROM jsonb_array_elements(commands) WITH ORDINALITY AS item(command, ordinal)
      WHERE command->>'command' <> 'deletebot'
    ), '[]'::jsonb),
    updated_at = now()
WHERE bot_user_id = 93372553;

UPDATE public.users
SET bot_info_version = bot_info_version + 1,
    updated_at = now()
WHERE id = 93372553;