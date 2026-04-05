import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { FileText, Info, Terminal } from "lucide-react";

import { getAdminConfig, type AdminConfig } from "@/api/client";
import { Skeleton } from "@/components/common/Skeleton";
import { useToastStore } from "@/stores/toast";

import styles from "./ConfigEditor.module.css";

const QUICK_COMMANDS = [
  {
    label: "Ver fontes disponíveis",
    cmd: "cd br-acc && cat docs/source_registry_br_v1.csv | head -20",
    desc: "Lista as primeiras 20 fontes do registry",
  },
  {
    label: "Rodar bootstrap completo",
    cmd: "cd br-acc && make bootstrap-all",
    desc: "Executa todas as 45 fontes do contrato (pode levar horas)",
  },
  {
    label: "Rodar apenas fontes core",
    cmd: 'cd br-acc && bash scripts/bootstrap_all_public.sh --sources "cnpj,comprasnet,pgfn,tcu,ceis,cnep,transparencia"',
    desc: "Executa as 7 fontes essenciais primeiro",
  },
  {
    label: "Rodar fonte individual",
    cmd: "cd br-acc && make etl-cnpj  # substitua 'cnpj' pelo pipeline_id",
    desc: "Executa ETL de uma única fonte",
  },
  {
    label: "Ver status do último bootstrap",
    cmd: "cat br-acc/audit-results/bootstrap-all/latest/summary.json",
    desc: "Mostra resultado da última execução completa",
  },
  {
    label: "Ver logs do container ETL",
    cmd: "cd br-acc && docker compose --profile etl logs etl",
    desc: "Últimos logs do container de ETL",
  },
];

export function ConfigEditor() {
  const { t } = useTranslation();
  const addToast = useToastStore((s) => s.addToast);
  const [config, setConfig] = useState<AdminConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [copiedIdx, setCopiedIdx] = useState<number | null>(null);

  useEffect(() => {
    getAdminConfig()
      .then((res) => setConfig(res))
      .catch(() => addToast("error", t("common.error")))
      .finally(() => setLoading(false));
  }, [addToast, t]);

  const copyCmd = (cmd: string, idx: number) => {
    navigator.clipboard.writeText(cmd);
    setCopiedIdx(idx);
    setTimeout(() => setCopiedIdx(null), 2000);
  };

  if (loading) {
    return (
      <div className={styles.loading}>
        <Skeleton variant="rect" height="32px" />
        <Skeleton variant="rect" height="32px" />
        <Skeleton variant="rect" height="32px" />
      </div>
    );
  }

  return (
    <div className={styles.container}>
      <section className={styles.section}>
        <h2 className={styles.sectionTitle}>
          <Info size={16} />
          {t("admin.howItWorks")}
        </h2>
        <div className={styles.steps}>
          <div className={styles.step}>
            <span className={styles.stepNum}>1</span>
            <div>
              <strong>{t("admin.step1Title")}</strong>
              <p className={styles.stepDesc}>{t("admin.step1Desc")}</p>
            </div>
          </div>
          <div className={styles.step}>
            <span className={styles.stepNum}>2</span>
            <div>
              <strong>{t("admin.step2Title")}</strong>
              <p className={styles.stepDesc}>{t("admin.step2Desc")}</p>
            </div>
          </div>
          <div className={styles.step}>
            <span className={styles.stepNum}>3</span>
            <div>
              <strong>{t("admin.step3Title")}</strong>
              <p className={styles.stepDesc}>{t("admin.step3Desc")}</p>
            </div>
          </div>
        </div>
      </section>

      <section className={styles.section}>
        <h2 className={styles.sectionTitle}>
          <Terminal size={16} />
          {t("admin.quickCommands")}
        </h2>
        <p className={styles.hint}>{t("admin.quickCommandsHint")}</p>
        <div className={styles.cmdList}>
          {QUICK_COMMANDS.map((item, idx) => (
            <div key={idx} className={styles.cmdItem}>
              <div className={styles.cmdHeader}>
                <span className={styles.cmdLabel}>{item.label}</span>
                <span className={styles.cmdDesc}>{item.desc}</span>
              </div>
              <div className={styles.cmdCode}>
                <code>{item.cmd}</code>
                <button
                  className={styles.copyBtn}
                  onClick={() => copyCmd(item.cmd, idx)}
                  title={copiedIdx === idx ? "Copiado!" : "Copiar"}
                >
                  {copiedIdx === idx ? "✓" : "📋"}
                </button>
              </div>
            </div>
          ))}
        </div>
      </section>

      {config && (
        <section className={styles.section}>
          <h2 className={styles.sectionTitle}>
            <FileText size={16} />
            {t("admin.currentConfig")}
          </h2>
          <div className={styles.configGrid}>
            <div className={styles.configCard}>
              <span className={styles.configValue}>{config.total_sources}</span>
              <span className={styles.configLabel}>{t("admin.totalInContract")}</span>
            </div>
            <div className={styles.configCard}>
              <span className={styles.configValue}>{config.core_sources.length}</span>
              <span className={styles.configLabel}>{t("admin.coreCount")}</span>
            </div>
          </div>
          <details className={styles.details}>
            <summary>{t("admin.seeCoreSources")}</summary>
            <div className={styles.chipList}>
              {config.core_sources.map((id) => (
                <span key={id} className={styles.chip}>
                  {id}
                </span>
              ))}
            </div>
          </details>
        </section>
      )}
    </div>
  );
}
