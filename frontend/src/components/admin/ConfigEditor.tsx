import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import { getAdminConfig, type AdminConfig } from "@/api/client";
import { Skeleton } from "@/components/common/Skeleton";
import { useToastStore } from "@/stores/toast";

import styles from "./ConfigEditor.module.css";

export function ConfigEditor() {
  const { t } = useTranslation();
  const addToast = useToastStore((s) => s.addToast);
  const [config, setConfig] = useState<AdminConfig | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getAdminConfig()
      .then((res) => setConfig(res))
      .catch(() => addToast("error", t("common.error")))
      .finally(() => setLoading(false));
  }, [addToast, t]);

  if (loading) {
    return (
      <div className={styles.loading}>
        <Skeleton variant="rect" height="32px" />
        <Skeleton variant="rect" height="32px" />
        <Skeleton variant="rect" height="32px" />
      </div>
    );
  }

  if (!config) return null;

  return (
    <div className={styles.container}>
      <section className={styles.section}>
        <h2 className={styles.sectionTitle}>{t("admin.coreSources")}</h2>
        <p className={styles.sectionDesc}>{t("admin.coreSourcesDesc")}</p>
        <div className={styles.chipList}>
          {config.core_sources.map((id) => (
            <span key={id} className={styles.chip}>
              {id}
            </span>
          ))}
        </div>
        <p className={styles.info}>
          {t("admin.totalSources")}: {config.total_sources}
        </p>
      </section>

      <section className={styles.section}>
        <h2 className={styles.sectionTitle}>{t("admin.notes")}</h2>
        <ul className={styles.notesList}>
          <li>{t("admin.note1")}</li>
          <li>{t("admin.note2")}</li>
          <li>{t("admin.note3")}</li>
        </ul>
      </section>
    </div>
  );
}
