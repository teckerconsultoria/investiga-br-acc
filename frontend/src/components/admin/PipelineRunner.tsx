import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Play, RotateCcw, Square } from "lucide-react";

import { listAdminSources, type AdminSource } from "@/api/client";
import { useAdminWebSocket } from "@/hooks/useAdminWebSocket";

import styles from "./PipelineRunner.module.css";

export function PipelineRunner() {
  const { t } = useTranslation();
  const [sources, setSources] = useState<AdminSource[]>([]);
  const [selectedSource, setSelectedSource] = useState("");
  const [resetDb, setResetDb] = useState(false);
  const [bootstrapSources, setBootstrapSources] = useState("");
  const logRef = useRef<HTMLDivElement>(null);

  const ws = useAdminWebSocket("changeme");

  useEffect(() => {
    listAdminSources()
      .then((res) => setSources(res.sources))
      .catch(() => {});
  }, []);

  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [ws.logs]);

  const handleRunPipeline = () => {
    if (!selectedSource) return;
    ws.runPipeline(selectedSource);
  };

  const handleRunBootstrap = () => {
    ws.runBootstrap(bootstrapSources, resetDb);
  };

  return (
    <div className={styles.container}>
      <div className={styles.grid}>
        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>{t("admin.runSingle")}</h2>
          <div className={styles.formRow}>
            <select
              className={styles.select}
              value={selectedSource}
              onChange={(e) => setSelectedSource(e.target.value)}
            >
              <option value="">{t("admin.selectSource")}</option>
              {sources.map((s) => (
                <option key={s.pipeline_id} value={s.pipeline_id}>
                  {s.name} ({s.pipeline_id})
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
        </section>

        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>{t("admin.runBootstrap")}</h2>
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
      </div>

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
            {ws.isConnected && !ws.isRunning && (
              <span className={styles.connected}>{t("admin.connected")}</span>
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
