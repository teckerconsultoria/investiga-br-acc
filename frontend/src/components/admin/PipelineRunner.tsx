import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Play, RotateCcw, Info, AlertTriangle, Square } from "lucide-react";

import { listAdminSources, type AdminSource } from "@/api/client";
import { useAdminWebSocket } from "@/hooks/useAdminWebSocket";

import styles from "./PipelineRunner.module.css";

interface PipelineRunnerProps {
  neo4jPassword: string;
  onPasswordChange: (v: string) => void;
  initialSource?: string;
}

export function PipelineRunner({ neo4jPassword, onPasswordChange, initialSource }: PipelineRunnerProps) {
  const { t } = useTranslation();
  const [sources, setSources] = useState<AdminSource[]>([]);
  const [selectedSource, setSelectedSource] = useState("");
  const [resetDb, setResetDb] = useState(false);
  const [bootstrapSources, setBootstrapSources] = useState("");
  const [quickMode, setQuickMode] = useState<"single" | "bootstrap" | "core">("single");
  const logRef = useRef<HTMLDivElement>(null);

  const ws = useAdminWebSocket(neo4jPassword);

  useEffect(() => {
    listAdminSources()
      .then((res) => setSources(res.sources))
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (initialSource) {
      setSelectedSource(initialSource);
      setQuickMode("single");
    }
  }, [initialSource]);

  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [ws.logs]);

  const runnableSources = sources.filter((s) => s.pipeline_id && s.implementation_state === "implemented");
  const selectedInfo = sources.find((s) => s.pipeline_id === selectedSource);

  const handleRunPipeline = () => {
    if (!selectedSource) return;
    ws.runPipeline(selectedSource);
  };

  const handleRunBootstrap = () => {
    ws.runBootstrap(bootstrapSources, resetDb);
  };

  const handleRunCore = () => {
    ws.runBootstrap("cnpj,comprasnet,pgfn,tcu,ceis,cnep,transparencia", false);
  };

  return (
    <div className={styles.container}>
      <div className={styles.passwordRow}>
        <label className={styles.passwordLabel}>{t("admin.neo4jPassword", "Senha Neo4j")}</label>
        <input
          className={styles.input}
          type="password"
          placeholder="••••••••••••••••"
          value={neo4jPassword}
          onChange={(e) => onPasswordChange(e.target.value)}
          disabled={ws.isRunning}
          style={{ flex: "0 0 280px" }}
        />
      </div>

      <div className={styles.modeTabs}>
        <button
          className={`${styles.modeTab} ${quickMode === "single" ? styles.modeTabActive : ""}`}
          onClick={() => setQuickMode("single")}
        >
          {t("admin.modeSingle")}
        </button>
        <button
          className={`${styles.modeTab} ${quickMode === "core" ? styles.modeTabActive : ""}`}
          onClick={() => setQuickMode("core")}
        >
          {t("admin.modeCore")}
        </button>
        <button
          className={`${styles.modeTab} ${quickMode === "bootstrap" ? styles.modeTabActive : ""}`}
          onClick={() => setQuickMode("bootstrap")}
        >
          {t("admin.modeBootstrap")}
        </button>
      </div>

      {quickMode === "single" && (
        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>{t("admin.runSingle")}</h2>
          <p className={styles.hint}>{t("admin.runSingleHint")}</p>
          <div className={styles.formRow}>
            <select
              className={styles.select}
              value={selectedSource}
              onChange={(e) => setSelectedSource(e.target.value)}
            >
              <option value="">{t("admin.selectSource")}</option>
              {runnableSources.map((s) => (
                <option key={s.pipeline_id} value={s.pipeline_id}>
                  {s.name} ({s.pipeline_id}) [{s.tier}]
                </option>
              ))}
            </select>
            <button
              className={styles.btnPrimary}
              onClick={handleRunPipeline}
              disabled={ws.isRunning || !selectedSource}
            >
              <Play size={16} />
              {t("admin.run")}
            </button>
          </div>
          {selectedInfo && (
            <div className={styles.sourceInfo}>
              <Info size={14} />
              <div>
                <strong>{selectedInfo.name}</strong> — {selectedInfo.category}
                <br />
                <span className={styles.muted}>
                  {selectedInfo.access_mode} · {selectedInfo.frequency} · {selectedInfo.status}
                </span>
              </div>
            </div>
          )}
          {runnableSources.length === 0 && (
            <div className={styles.warning}>
              <AlertTriangle size={16} />
              <span>{t("admin.noRunnableSources")}</span>
            </div>
          )}
        </section>
      )}

      {quickMode === "core" && (
        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>{t("admin.runCore")}</h2>
          <p className={styles.hint}>{t("admin.runCoreHint")}</p>
          <div className={styles.coreList}>
            {sources.filter((s) => ["cnpj", "comprasnet", "pgfn", "tcu", "ceis", "cnep", "transparencia"].includes(s.pipeline_id)).map((s) => (
              <span key={s.pipeline_id} className={styles.coreChip}>
                {s.name}
              </span>
            ))}
          </div>
          <button
            className={styles.btnPrimary}
            onClick={handleRunCore}
            disabled={ws.isRunning}
            style={{ marginTop: "var(--space-md)" }}
          >
            <Play size={16} />
            {t("admin.runCore")}
          </button>
        </section>
      )}

      {quickMode === "bootstrap" && (
        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>{t("admin.runBootstrap")}</h2>
          <p className={styles.hint}>{t("admin.runBootstrapHint")}</p>
          <div className={styles.formRow}>
            <input
              className={styles.input}
              type="text"
              placeholder={t("admin.bootstrapSourcesPlaceholder")}
              value={bootstrapSources}
              onChange={(e) => setBootstrapSources(e.target.value)}
            />
            <label className={styles.checkbox}>
              <input
                type="checkbox"
                checked={resetDb}
                onChange={(e) => setResetDb(e.target.checked)}
              />
              {t("admin.resetDb")}
            </label>
            <button
              className={styles.btnDanger}
              onClick={handleRunBootstrap}
              disabled={ws.isRunning}
            >
              <RotateCcw size={16} />
              {t("admin.bootstrap")}
            </button>
          </div>
        </section>
      )}

      <section className={styles.logSection}>
        <div className={styles.logHeader}>
          <h2 className={styles.sectionTitle}>{t("admin.log")}</h2>
          <div className={styles.logStatus}>
            {ws.isRunning && (
              <span className={styles.running}>
                <Square size={12} />
                {t("admin.running")}
              </span>
            )}
          </div>
        </div>
        <div className={styles.logContainer} ref={logRef}>
          {ws.logs.length === 0 ? (
            <p className={styles.logEmpty}>{t("admin.logEmpty")}</p>
          ) : (
            ws.logs.map((entry, i) => (
              <div
                key={i}
                className={`${styles.logLine} ${
                  entry.type === "error"
                    ? styles.logError
                    : entry.type === "end"
                    ? styles.logEnd
                    : ""
                }`}
              >
                {entry.source && (
                  <span className={styles.logSource}>[{entry.source}]</span>
                )}
                {entry.line || entry.message || JSON.stringify(entry)}
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}

