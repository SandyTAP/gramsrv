import { useI18n } from "../i18n";
import type { Navigate } from "../routing";
import { AppLink } from "./AppLink";

// SectionTabs renders the in-page sub-menu shared by the consolidated sidebar
// entries (Grants, Verification, Star Gifts, Media, NFT Items): the sidebar
// link is flat, and the tabs inside the page switch between the sibling
// routes. Same visual language as Server Settings' Settings/Services bar.
export type SectionTab = { path: string; labelKey: string };

export function SectionTabs({
  tabs,
  active,
  navigate
}: {
  tabs: SectionTab[];
  active: string;
  navigate: Navigate;
}) {
  const { t } = useI18n();
  if (tabs.length < 2) {
    return null;
  }
  return (
    <div className="tab-bar" role="tablist" aria-label={t("sectionTabs.label")}>
      {tabs.map((tab) => (
        <AppLink
          key={tab.path}
          href={tab.path}
          navigate={navigate}
          className={`tab-btn ${active === tab.path ? "active" : ""}`}
        >
          {t(tab.labelKey)}
        </AppLink>
      ))}
    </div>
  );
}

export const grantsTabs: SectionTab[] = [
  { path: "/monetization", labelKey: "layout.premium" },
  { path: "/star-issue", labelKey: "layout.starIssue" },
  { path: "/give-gifts", labelKey: "layout.giveGifts" }
];

export const giftTabs: SectionTab[] = [
  { path: "/gifts", labelKey: "gifts.pageTitle" },
  { path: "/auctions", labelKey: "layout.auctions" }
];

export const mediaTabs: SectionTab[] = [
  { path: "/stickers", labelKey: "layout.stickers" },
  { path: "/emoji", labelKey: "layout.emoji" },
  { path: "/gif-catalog", labelKey: "layout.gifCatalog" }
];

export const nftTabs: SectionTab[] = [
  { path: "/collectible-usernames", labelKey: "layout.collectibleUsernames" },
  { path: "/collectible-phones", labelKey: "layout.collectiblePhones" },
  { path: "/nft-gifts", labelKey: "nft.gifts" }
];
