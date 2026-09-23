import { Loader2, ShieldCheck, Upload, X } from "lucide-react";
import { useState } from "react";
import { createPortal } from "react-dom";
import { APIError, api, errorMessage } from "../api";
import { Alert } from "../components/ui";
import { useI18n } from "../i18n";
import type { CommandResult } from "../types";

type PackOutcome = { title: string; status: string; gift_id?: string; error?: string };

export function GiftPackModal({ onClose, onImported }: { onClose: () => void; onImported: () => void }) {
  const { t } = useI18n();
  const [file, setFile] = useState<File | null>(null);
  const [reason, setReason] = useState("");
  const [preview, setPreview] = useState<CommandResult | null>(null);
  const [result, setResult] = useState<CommandResult | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  function form(confirm: boolean) {
    const data = new FormData();
    if (file) data.append("file", file);
    data.append("metadata", JSON.stringify({ reason, confirm, command_id: confirm ? preview?.command_id : undefined }));
    return data;
  }

  async function validate() {
    if (!file || !reason.trim()) return;
    setBusy(true); setError(""); setPreview(null); setResult(null);
    try { setPreview(await api.importGiftPack(form(false))); }
    catch (err) { setError(errorMessage(err)); }
    finally { setBusy(false); }
  }

  async function confirm() {
    if (!preview || !file) return;
    setBusy(true); setError("");
    try {
      setResult(await api.importGiftPack(form(true)));
      setPreview(null);
      onImported();
    } catch (err) {
      if (err instanceof APIError && err.result && typeof err.result === "object" && "details" in err.result) {
        setResult(err.result as CommandResult);
        setPreview(null);
      }
      setError(errorMessage(err)); onImported();
    }
    finally { setBusy(false); }
  }

  const shown = result ?? preview;
  const gifts = (shown?.details?.gifts ?? []) as PackOutcome[];
  return createPortal(
    <div className="modal-backdrop" role="presentation">
      <section className="modal command-modal gift-import-modal" role="dialog" aria-modal="true" aria-label={t("gifts.pack.title")}>
        <div className="modal-head">
          <div><div className="eyebrow">{t("gifts.importEyebrow")}</div><h2>{t("gifts.pack.title")}</h2></div>
          <button className="icon-btn" type="button" onClick={onClose} disabled={busy} aria-label={t("action.close")}><X size={15} /></button>
        </div>
        <div className="command-body gift-import-modal-body">
          <p>{t("gifts.pack.hint")}</p>
          <label><span>{t("gifts.pack.file")}</span><input type="file" accept=".zip,application/zip" onChange={(event) => { setFile(event.target.files?.[0] ?? null); setPreview(null); setResult(null); }} /></label>
          {file && <div className="muted">{file.name} · {(file.size / 1048576).toFixed(1)} MB</div>}
          <label className="gift-reason-field"><span>{t("gifts.reason")}</span><input value={reason} onChange={(event) => { setReason(event.target.value); setPreview(null); setResult(null); }} placeholder={t("gifts.reasonPlaceholder")} /></label>
          {error && <Alert>{error}</Alert>}
          {shown && <div className="gift-validation">
            <strong>{result ? t("gifts.pack.result") : t("gifts.pack.preview")}: {String(shown.details?.pack_name ?? "")}</strong>
            <div className="mono">SHA-256: {String(shown.details?.content_sha256 ?? "")}</div>
            <ul>{gifts.map((gift, index) => <li key={`${gift.title}-${index}`}>
              <strong>{gift.title}</strong> — {t(`gifts.pack.${gift.status}`)} {gift.gift_id ? `#${gift.gift_id}` : ""}{gift.error ? `: ${gift.error}` : ""}
            </li>)}</ul>
          </div>}
        </div>
        <div className="modal-actions">
          <button className="btn" type="button" onClick={onClose} disabled={busy}>{t("common.close")}</button>
          <button className="btn" type="button" onClick={validate} disabled={busy || !file || !reason.trim()}>{busy ? <Loader2 className="spin" size={15} /> : <ShieldCheck size={15} />}{t("gifts.validate")}</button>
          <button className="btn primary" type="button" onClick={confirm} disabled={busy || !preview}><Upload size={15} />{t("gifts.confirmImport")}</button>
        </div>
      </section>
    </div>, document.body
  );
}
