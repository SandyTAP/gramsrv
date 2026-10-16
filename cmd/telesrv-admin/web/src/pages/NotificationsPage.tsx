import { BellRing, Loader2, Send } from "lucide-react";
import { useState } from "react";
import { api, errorMessage } from "../api";
import { ChannelPicker, UserPicker } from "../components/EntityPicker";
import { Alert, JsonBlock } from "../components/ui";
import { useI18n } from "../i18n";
import type { AccountRow, ChannelRow, CommandResult } from "../types";


// The account every notification is sent from. Shown in the UI because an
// operator is about to speak as somebody: the whole point of this page is that
// the message arrives looking like a person, not like this panel.
const SENDER_USERNAME = "@AbuseNotifications";
const MAX_TEXT = 4096;

// Notification templates.
//
// These are the notices an operator actually sends, written once so the wording
// stays consistent and nobody has to retype "your channel was blocked" at 3am.
// Each carries its own audit reason, because the reason is what a later reader
// of the audit log needs to know, and a generic "notification" never does.
//
// Telegram legacy Markdown is supported on send, so **bold** renders as bold
// rather than showing asterisks in someone's private dialog.
type Template = {
  id: string;
  label: string;
  reason: string;
  text: string;
};

const TEMPLATES: Template[] = [
  {
    id: "blocked",
    label: "Блокировка",
    reason: "channel blocked",
    text:
      "*Ваш канал заблокирован*\n\n" +
      "Канал *@{channel}* отправлен на проверку модераторами и временно недоступен для новых подписчиков.\n\n" +
      "Что происходит:\n" +
      "• Канал скрыт из поиска\n" +
      "• Новые участники не могут подписаться\n" +
      "• Публикации приостановлены\n\n" +
      "Мы свяжемся с вами, когда проверка завершится. Если у вас есть вопросы — ответьте на это сообщение."
  },
  {
    id: "unblocked",
    label: "Разблокировка",
    reason: "channel unblocked",
    text:
      "*Проверка завершена — ограничения сняты*\n\n" +
      "Канал *@{channel}* снова работает в обычном режиме:\n\n" +
      "✅ Ограничения сняты\n" +
      "✅ Канал снова виден в поиске\n" +
      "✅ Подписчики вернулись\n\n" +
      "Спасибо за терпение!"
  },
  {
    id: "extra_check",
    label: "Дополнительная проверка",
    reason: "additional review",
    text:
      "*Требуется дополнительная проверка*\n\n" +
      "Модераторы XiroGram проводят дополнительную проверку канала *@{channel}*.\n\n" +
      "Это стандартная процедура: она занимает до 24 часов и не означает нарушений с вашей стороны.\n\n" +
      "Пожалуйста, не меняйте оформление и описание канала до окончания проверки — так мы сможем оценить канал в том виде, в каком его видят новые участники."
  },
  {
    id: "moderator",
    label: "Сообщение от модератора",
    reason: "moderator message",
    text:
      "*Сообщение от модерации*\n\n" +
      "Канал *@{channel}*\n\n" +
      "{message}"
  },
  {
    id: "will_delete",
    label: "Канал будет удалён",
    reason: "channel pending deletion",
    text:
      "*Канал будет удалён*\n\n" +
      "Канал *@{channel}* нарушает правила платформы и будет удалён.\n\n" +
      "Если вы считаете это ошибкой, напишите в течение 7 дней — канал будет восстановлен до окончания удаления, и мы пересмотрим решение.\n\n" +
      "После этого срока восстановление невозможно."
  },
  {
    id: "permanent_ban",
    label: "Заблокирован навсегда",
    reason: "channel permanently banned",
    text:
      "*Канал заблокирован навсегда*\n\n" +
      "Канал *@{channel}* заблокирован за систематические нарушения правил платформы.\n\n" +
      "Решение окончательное: восстановление не предусмотрено.\n\n" +
      "Все связанные аккаунты также ограничены. Попытки обхода через новые аккаунты приведут к блокировке."
  }
];

export function NotificationsPage() {
  const { t } = useI18n();
  const [recipient, setRecipient] = useState<AccountRow | null>(null);
  const [channel, setChannel] = useState<ChannelRow | null>(null);
  const [text, setText] = useState("");
  const [reason, setReason] = useState("");
  const [confirm, setConfirm] = useState(false);
  const [preview, setPreview] = useState<CommandResult | null>(null);
  const [result, setResult] = useState<CommandResult | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const recipientID = recipient?.ID ?? 0;
  const trimmed = text.trim();
  const tooLong = trimmed.length > MAX_TEXT;

  // Templates name the channel as @channel so the operator never types a handle
  // into six templates by hand. When no channel is picked the placeholder is
  // replaced with a generic word rather than left as literal "@{channel}",
  // which would read as a bug to whoever receives it.
  const applyChannelHandle = (value: string) => {
    const handle = (channel?.Username || "").replace(/^@/, "");
    if (!handle) {
      return value.replace(/@?\{channel\}/g, "ваш канал");
    }
    return value.replace(/@?\{channel\}/g, handle);
  };

  // The notice opens with the recipient's own handle. A moderation message that
  // starts with the person's name is unmistakably addressed to them, which is
  // the point of sending it from a real account rather than a system notice.
  const composeMessage = (value: string) => {
    const who = (recipient?.Username || "").replace(/^@/, "");
    const body = applyChannelHandle(value);
    return who ? `@${who}\n\n${body}` : body;
  };

  const send = async () => {
    setBusy(true);
    setError("");
    setResult(null);
    setPreview(null);
    try {
      const res = await api.action("/api/actions/notification", {
        user_id: recipientID,
        text: composeMessage(trimmed),
        reason: reason.trim() || "operator notification",
        confirm
      });
      // The first call is always a dry run: a notification is addressed to a
      // person and cannot be unsent, so confirm stays the operator's decision.
      if (confirm) setResult(res);
      else setPreview(res);
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setBusy(false);
    }
  };

  const reset = () => {
    setPreview(null);
    setChannel(null);
    setResult(null);
    setConfirm(false);
    setText("");
    setReason("");
  };

  return (
    <div className="page">
      <div className="page-head">
        <h1>
          <BellRing size={20} /> {t("route.notifications")}
        </h1>
        <p className="muted">{t("notifications.subtitle")}</p>
      </div>

      <Alert>{t("notifications.senderNotice", { sender: SENDER_USERNAME })}</Alert>

      <div className="panel">
        <UserPicker label={t("notifications.recipient")} value={recipient} onChange={setRecipient} />

        <ChannelPicker label={t("notifications.channel")} value={channel} onChange={setChannel} />

        <div className="field">
          <span>{t("notifications.templates")}</span>
          <div className="template-row">
            {TEMPLATES.map((tpl) => (
              <button
                key={tpl.id}
                type="button"
                className={`template-chip${text === tpl.text ? " active" : ""}`}
                onClick={() => {
                  setText(tpl.text);
                  setReason(tpl.reason);
                  // A template change invalidates any earlier preview: the dry
                  // run on screen must always describe what would be sent.
                  setPreview(null);
                  setResult(null);
                  setConfirm(false);
                }}
              >
                {tpl.label}
              </button>
            ))}
          </div>
        </div>

        <label className="field">
          <span>{t("notifications.text")}</span>
          <textarea
            rows={6}
            value={text}
            onChange={(e) => {
              setText(e.target.value);
              setPreview(null);
              setResult(null);
              setConfirm(false);
            }}
            placeholder={t("notifications.textPlaceholder")}
          />
          <small className={tooLong ? "error" : "muted"}>
            {trimmed.length} / {MAX_TEXT}
          </small>
        </label>

        <label className="field">
          <span>{t("notifications.reason")}</span>
          <input
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder={t("notifications.reasonPlaceholder")}
          />
          <small className="muted">{t("notifications.reasonHint")}</small>
        </label>

        <div className="row">
          <button
            type="button"
            disabled={busy || !recipientID || !trimmed || tooLong}
            onClick={() => {
              // Any edit invalidates a preview: confirming a dry run that no
              // longer matches the form is how the wrong text gets sent.
              setConfirm(true);
              void send();
            }}
          >
            {busy ? <Loader2 size={16} className="spin" /> : <Send size={16} />}
            {t("notifications.send")}
          </button>
          {(preview || result) && (
            <button type="button" className="ghost" onClick={reset}>
              {t("common.clear")}
            </button>
          )}
        </div>
      </div>

      {error && <Alert>{error}</Alert>}

      {preview && !result && (
        <div className="panel">
          <Alert>{t("notifications.previewNotice")}</Alert>
          <JsonBlock value={JSON.stringify(preview, null, 2)} />
        </div>
      )}

      {result && (
        <div className="panel">
          <Alert>
            {t("notifications.sentNotice", { name: recipient?.Username || recipientID })}
          </Alert>
          <JsonBlock value={JSON.stringify(result, null, 2)} />
        </div>
      )}
    </div>
  );
}