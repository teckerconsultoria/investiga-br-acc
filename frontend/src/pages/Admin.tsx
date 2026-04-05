import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";

import { SourceTable } from "@/components/admin/SourceTable";
import { PipelineRunner } from "@/components/admin/PipelineRunner";
import { ConfigEditor } from "@/components/admin/ConfigEditor";

import styles from "./Admin.module.css";

export function Admin() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState<"sources" | "run" | "config">("sources");
  const [neo4jPassword, setNeo4jPassword] = useState("");
  const [initialSource, setInitialSource] = useState<string | undefined>();
  const [downloadMode, setDownloadMode] = useState(false);

  const tabs = [
    { key: "sources" as const, label: t("admin.tabSources") },
    { key: "run" as const, label: t("admin.tabRun") },
    { key: "config" as const, label: t("admin.tabConfig") },
  ];

  const handleRunSource = useCallback((pipelineId: string) => {
    setInitialSource(pipelineId);
    setDownloadMode(false);
    setActiveTab("run");
  }, []);

  const handleDownloadSource = useCallback((pipelineId: string) => {
    setInitialSource(pipelineId);
    setDownloadMode(true);
    setActiveTab("run");
  }, []);

  return (
    <div className={styles.page}>
      <h1 className={styles.title}>{t("admin.title")}</h1>

      <nav className={styles.tabs}>
        {tabs.map((tab) => (
          <button
            key={tab.key}
            className={`${styles.tab} ${activeTab === tab.key ? styles.tabActive : ""}`}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      <div className={styles.content}>
        {activeTab === "sources" && <SourceTable onRun={handleRunSource} onDownload={handleDownloadSource} />}
        {activeTab === "run" && (
          <PipelineRunner
            neo4jPassword={neo4jPassword}
            onPasswordChange={setNeo4jPassword}
            initialSource={initialSource}
            downloadMode={downloadMode}
          />
        )}
        {activeTab === "config" && <ConfigEditor />}
      </div>
    </div>
  );
}
