import { Users } from "lucide-react";
import { useState } from "react";
import { ActionButton } from "../components/ActionButton";
import { SectionTabs, grantsTabs } from "../components/SectionTabs";
import { PageFrame } from "../components/ui";
import { useI18n } from "../i18n";
import type { Navigate } from "../routing";

// StarIssuePage — массовая выдача Звёзд сразу всем реальным аккаунтам
// (без ботов и системных). Отдельная вкладка, чтобы не загромождать
// per-user операции на странице «Звёзды и Premium».
export function StarIssuePage({ navigate }: { navigate: Navigate }) {
  const { t } = useI18n();
  const [amount, setAmount] = useState("100");

  const parsed = Number(amount);
  const canIssue = Number.isSafeInteger(parsed) && parsed > 0;

  return (
    <PageFrame title={t("starIssue.title")} eyebrow={t("starIssue.eyebrow")}>
      <SectionTabs tabs={grantsTabs} active="/star-issue" navigate={navigate} />
      <section className="surface premium-operations-compact">
        <div className="premium-mass-grant">
          <div className="premium-mass-grant-head">
            <span><Users size={14} /></span><strong>{t("premium.grantStarsAllTitle")}</strong>
          </div>
          <div className="premium-mass-grant-control">
            <input aria-label={t("premium.starsAmount")} type="number" min={1} value={amount}
              onChange={(event) => setAmount(event.target.value)} />
          </div>
          <div className="premium-mass-grant-actions">
            <ActionButton compact tone="warn" icon={<Users size={14} />} label={t("premium.grantStarsAll")}
              path="/api/actions/grant-stars-all"
              disabled={!canIssue}
              payload={() => ({ amount: parsed })} />
            <p className="premium-store-hint">{t("premium.grantStarsAllHint")}</p>
          </div>
        </div>
      </section>
    </PageFrame>
  );
}