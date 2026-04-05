import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Play, Download } from "lucide-react";

import { listAdminSources, type AdminSource } from "@/api/client";
import { Skeleton } from "@/components/common/Skeleton";
import { useToastStore } from "@/stores/toast";

import styles from "./SourceTable.module.css";

const TIER_COLORS: Record<string, string> = {
  P0: "var(--accent)",
  P1: "var(--system-cyan)",
  P2: "var(--text-secondary)",
  P3: "var(--text-muted)",
};

const STATUS_COLORS: Record<string, string> = {
  loaded: "var(--system-cyan)",
  healthy: "var(--system-cyan)",
  partial: "var(--accent)",
  stale: "var(--system-amber)",
  blocked_external: "var(--system-red)",
  not_built: "var(--text-muted)",
};

interface SourceTableProps {
  onRun?: (pipelineId: string) => void;
  onDownload?: (pipelineId: string) => void;
}

export function SourceTable({ onRun, onDownload }: SourceTableProps) {
  const { t } = useTranslation();
  const addToast = useToastStore((s) => s.addToast);
  const [sources, setSources] = useState<AdminSource[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterTier, setFilterTier] = useState("");
  const [filterCategory, setFilterCategory] = useState("");
  const [filterStatus, setFilterStatus] = useState("");

  useEffect(() => {
    listAdminSources()
      .then((res) => setSources(res.sources))
      .catch(() => addToast("error", t("common.error")))
      .finally(() => setLoading(false));
  }, [addToast, t]);

  const categories = [...new Set(sources.map((s) => s.category))].sort();
  const tiers = [...new Set(sources.map((s) => s.tier))].sort();
  const statuses = [...new Set(sources.map((s) => s.status))].sort();

  const filtered = sources.filter((s) => {
    if (filterTier && s.tier !== filterTier) return false;
    if (filterCategory && s.category !== filterCategory) return false;
    if (filterStatus && s.status !== filterStatus) return false;
    return true;
  });

  if (loading) {
    return (
      <div className={styles.loading}>
        <Skeleton variant="rect" height="40px" />
        <Skeleton variant="rect" height="40px" />
        <Skeleton variant="rect" height="40px" />
        <Skeleton variant="rect" height="40px" />
        <Skeleton variant="rect" height="40px" />
      </div>
    );
  }

  return (
    <div className={styles.container}>
      <div className={styles.filters}>
        <select
          className={styles.filterSelect}
          value={filterTier}
          onChange={(e) => setFilterTier(e.target.value)}
        >
          <option value="">{t("admin.allTiers")}</option>
          {tiers.map((tier) => (
            <option key={tier} value={tier}>{tier}</option>
          ))}
        </select>

        <select
          className={styles.filterSelect}
          value={filterCategory}
          onChange={(e) => setFilterCategory(e.target.value)}
        >
          <option value="">{t("admin.allCategories")}</option>
          {categories.map((cat) => (
            <option key={cat} value={cat}>{cat}</option>
          ))}
        </select>

        <select
          className={styles.filterSelect}
          value={filterStatus}
          onChange={(e) => setFilterStatus(e.target.value)}
        >
          <option value="">{t("admin.allStatuses")}</option>
          {statuses.map((st) => (
            <option key={st} value={st}>{st}</option>
          ))}
        </select>

        <span className={styles.count}>
          {filtered.length} / {sources.length} {t("admin.sources")}
        </span>
      </div>

      <div className={styles.tableWrapper}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th className={styles.th}>{t("admin.colSource")}</th>
              <th className={styles.th}>{t("admin.colCategory")}</th>
              <th className={styles.th}>{t("admin.colTier")}</th>
              <th className={styles.th}>{t("admin.colStatus")}</th>
              <th className={styles.th}>{t("admin.colQuality")}</th>
              <th className={styles.th}>{t("admin.colFrequency")}</th>
              <th className={styles.th}>{t("admin.colMode")}</th>
              {onDownload && <th className={styles.th}></th>}
              {onRun && <th className={styles.th}></th>}
            </tr>
          </thead>
          <tbody>
            {filtered.map((source) => (
              <tr key={source.pipeline_id} className={styles.tr}>
                <td className={styles.td}>
                  <div className={styles.nameCell}>
                    <span className={styles.name}>{source.name}</span>
                    {source.primary_url && (
                      <a
                        href={source.primary_url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className={styles.url}
                      >
                        {source.primary_url}
                      </a>
                    )}
                  </div>
                </td>
                <td className={styles.td}>{source.category}</td>
                <td className={styles.td}>
                  <span
                    className={styles.badge}
                    style={{ color: TIER_COLORS[source.tier] || "var(--text-primary)" }}
                  >
                    {source.tier}
                  </span>
                </td>
                <td className={styles.td}>
                  <span
                    className={styles.badge}
                    style={{ color: STATUS_COLORS[source.status] || "var(--text-primary)" }}
                  >
                    {source.status}
                  </span>
                </td>
                <td className={styles.td}>{source.quality_status || "-"}</td>
                <td className={styles.td}>{source.frequency}</td>
                <td className={styles.td}>{source.access_mode}</td>
                {onDownload && (
                  <td className={styles.td}>
                    {source.access_mode === "file" && source.pipeline_id && (
                      <button
                        className={styles.btnDownload}
                        onClick={() => onDownload(source.pipeline_id)}
                        title={t("admin.download", "Baixar dados")}
                      >
                        <Download size={12} />
                      </button>
                    )}
                  </td>
                )}
                {onRun && (
                  <td className={styles.td}>
                    {source.implementation_state === "implemented" && source.pipeline_id && (
                      <button
                        className={styles.btnRun}
                        onClick={() => onRun(source.pipeline_id)}
                        title={t("admin.run", "Executar")}
                      >
                        <Play size={12} />
                      </button>
                    )}
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
