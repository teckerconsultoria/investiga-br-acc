import { useState, useEffect, useRef } from "react";

// ── Paleta e fontes ──────────────────────────────────────────────
const CSS = `
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@300;400;500&display=swap');

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg:       #080b0f;
    --surface:  #0f1318;
    --border:   #1e2530;
    --muted:    #2a3340;
    --text:     #c8d4e0;
    --bright:   #eaf2ff;
    --amber:    #f5a623;
    --green:    #2dca73;
    --red:      #e8455a;
    --orange:   #f07d2a;
    --blue:     #4a9eff;
    --mono:     'JetBrains Mono', monospace;
    --sans:     'Syne', sans-serif;
  }

  body { background: var(--bg); color: var(--text); font-family: var(--mono); }

  ::-webkit-scrollbar { width: 4px; }
  ::-webkit-scrollbar-track { background: var(--bg); }
  ::-webkit-scrollbar-thumb { background: var(--muted); border-radius: 2px; }
`;

// ── Dados mock ───────────────────────────────────────────────────
const MOCK_RESULT = {
  cnpj: "11.222.333/0001-81",
  razao_social: "CONSTRUTORA ALFA BRASIL LTDA",
  nome_fantasia: "ALFA BRASIL",
  situacao_cadastral: "ATIVA",
  data_abertura: "2008-11-04",
  porte: "MÉDIO",
  capital_social: 1200000,
  socios: [
    { nome: "ROBERTO SILVA MENDES", qualificacao: "Sócio-Administrador", desde: "2008-11-04" },
    { nome: "CARLA MENDES FERREIRA", qualificacao: "Sócia", desde: "2015-03-22" },
  ],
  contratos_publicos: {
    total_contratos: 14,
    valor_total_brl: 4200000,
    orgaos_contratantes: ["Ministério da Saúde", "FNDE", "Prefeitura de Campinas"],
    ultimo_contrato: "2024-08-01",
  },
  sancoes: {
    ceis: false,
    cnep: true,
    detalhes: [
      { lista: "CNEP", motivo: "Acordo de Leniência", data: "2022-05-10", orgao: "CGU", vigencia: "2027-05-10" },
    ],
  },
  score: {
    valor: 31,
    classificacao: "ALTO RISCO",
    fatores: [
      { fator: "Registro ativo no CNEP", impacto: "negativo", peso: "alto" },
      { fator: "Contratos federais ativos", impacto: "positivo", peso: "médio" },
      { fator: "Situação cadastral regular", impacto: "positivo", peso: "alto" },
      { fator: "Sócios sem restrições individuais", impacto: "positivo", peso: "baixo" },
      { fator: "Capital social abaixo do setor", impacto: "negativo", peso: "baixo" },
    ],
  },
  narrativa:
    "A Construtora Alfa Brasil Ltda está ativa desde 2008 e acumulou R$ 4,2 milhões em 14 contratos federais junto ao Ministério da Saúde e FNDE. No entanto, possui registro ativo no CNEP decorrente de acordo de leniência firmado com a CGU em 2022, com vigência até 2027. Esse fato representa risco jurídico significativo para novas contratações públicas e operações de crédito. Recomenda-se verificação jurídica aprofundada e análise do acordo antes de qualquer relação contratual.",
  fontes: ["Receita Federal", "Portal da Transparência", "CNEP"],
  consultado_em: new Date().toISOString(),
  cache: false,
};

const API_SOURCES = [
  { id: "receita", nome: "Receita Federal", sub: "BrasilAPI" },
  { id: "transparencia", nome: "Portal da Transparência", sub: "CGU" },
  { id: "ceis", nome: "CEIS", sub: "CGU" },
  { id: "cnep", nome: "CNEP", sub: "CGU" },
  { id: "openrouter", nome: "OpenRouter", sub: "LLM" },
];

// ── Helpers ──────────────────────────────────────────────────────
function fmtBRL(v) {
  return new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(v);
}
function fmtDate(s) {
  if (!s) return "—";
  const [y, m, d] = s.split("-");
  return `${d}/${m}/${y}`;
}
function maskCNPJ(v) {
  return v
    .replace(/\D/g, "")
    .slice(0, 14)
    .replace(/^(\d{2})(\d)/, "$1.$2")
    .replace(/^(\d{2})\.(\d{3})(\d)/, "$1.$2.$3")
    .replace(/\.(\d{3})(\d)/, ".$1/$2")
    .replace(/(\d{4})(\d)/, "$1-$2");
}

// ── Score gauge ──────────────────────────────────────────────────
function ScoreGauge({ value, label }) {
  const color =
    value >= 80 ? "var(--green)" :
    value >= 60 ? "#c8d423" :
    value >= 40 ? "var(--orange)" : "var(--red)";

  const pct = value / 100;
  const r = 42;
  const circ = 2 * Math.PI * r;
  const dash = circ * 0.75;
  const gap = circ * 0.25;
  const offset = dash - pct * dash;

  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 8 }}>
      <svg width={110} height={90} viewBox="0 0 110 90">
        <circle cx={55} cy={65} r={r} fill="none" stroke="var(--muted)" strokeWidth={8}
          strokeDasharray={`${dash} ${gap}`} strokeLinecap="round"
          transform="rotate(-225 55 65)" />
        <circle cx={55} cy={65} r={r} fill="none" stroke={color} strokeWidth={8}
          strokeDasharray={`${dash - offset} ${circ - (dash - offset)}`}
          strokeLinecap="round" transform="rotate(-225 55 65)"
          style={{ transition: "stroke-dasharray 1s ease" }} />
        <text x={55} y={60} textAnchor="middle" fill={color}
          style={{ fontSize: 22, fontFamily: "var(--mono)", fontWeight: 500 }}>{value}</text>
        <text x={55} y={75} textAnchor="middle" fill="var(--text)"
          style={{ fontSize: 8, fontFamily: "var(--mono)", letterSpacing: 1 }}>/ 100</text>
      </svg>
      <span style={{ color, fontFamily: "var(--sans)", fontWeight: 700, fontSize: 11, letterSpacing: 2 }}>
        {label}
      </span>
    </div>
  );
}

// ── Collapsible ──────────────────────────────────────────────────
function Section({ title, badge, badgeColor, children, defaultOpen = false }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div style={{ borderTop: "1px solid var(--border)", marginTop: 2 }}>
      <button onClick={() => setOpen(!open)}
        style={{
          width: "100%", background: "none", border: "none", cursor: "pointer",
          padding: "14px 0", display: "flex", alignItems: "center", gap: 10,
          color: "var(--bright)", fontFamily: "var(--sans)", fontWeight: 600,
          fontSize: 12, letterSpacing: 1.5, textTransform: "uppercase",
        }}>
        <span style={{ flex: 1, textAlign: "left" }}>{title}</span>
        {badge && (
          <span style={{
            background: badgeColor || "var(--muted)", color: "var(--bright)",
            fontSize: 9, padding: "2px 8px", borderRadius: 2, letterSpacing: 1,
          }}>{badge}</span>
        )}
        <span style={{ color: "var(--muted)", fontSize: 16, lineHeight: 1 }}>{open ? "−" : "+"}</span>
      </button>
      {open && <div style={{ paddingBottom: 16 }}>{children}</div>}
    </div>
  );
}

// ── Status dot ───────────────────────────────────────────────────
function StatusDot({ status }) {
  const colors = { OK: "var(--green)", LENTO: "var(--amber)", INDISPONÍVEL: "var(--red)", VERIFICANDO: "var(--muted)" };
  return (
    <span style={{
      display: "inline-block", width: 7, height: 7, borderRadius: "50%",
      background: colors[status] || "var(--muted)",
      boxShadow: status === "OK" ? `0 0 6px ${colors.OK}` : "none",
    }} />
  );
}

// ── Export helpers ───────────────────────────────────────────────
function exportJSON(data) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = `vigiagov_${data.cnpj.replace(/\D/g, "")}.json`;
  a.click(); URL.revokeObjectURL(url);
}

function exportText(data) {
  const txt = `VIGIAGOV — RELATÓRIO DE CONSULTA
======================================
CNPJ: ${data.cnpj}
Empresa: ${data.razao_social}
Situação: ${data.situacao_cadastral}
Score de Risco: ${data.score.valor}/100 — ${data.score.classificacao}
Consultado em: ${new Date(data.consultado_em).toLocaleString("pt-BR")}

NARRATIVA
---------
${data.narrativa}

CONTRATOS PÚBLICOS
------------------
Total: ${data.contratos_publicos.total_contratos} contratos
Valor total: ${fmtBRL(data.contratos_publicos.valor_total_brl)}
Órgãos: ${data.contratos_publicos.orgaos_contratantes.join(", ")}

SANÇÕES
-------
CEIS: ${data.sancoes.ceis ? "SIM" : "NÃO"}
CNEP: ${data.sancoes.cnep ? "SIM" : "NÃO"}
${data.sancoes.detalhes.map(d => `  • ${d.lista}: ${d.motivo} (${fmtDate(d.data)})`).join("\n")}

FATORES DO SCORE
----------------
${data.score.fatores.map(f => `  [${f.impacto.toUpperCase()}] ${f.fator} (peso: ${f.peso})`).join("\n")}

Fontes: ${data.fontes.join(", ")}
`;
  const blob = new Blob([txt], { type: "text/plain" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url; a.download = `vigiagov_${data.cnpj.replace(/\D/g, "")}.txt`;
  a.click(); URL.revokeObjectURL(url);
}

// ── Main App ─────────────────────────────────────────────────────
export default function VigiaGov() {
  const [cnpj, setCnpj] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [apiStatus, setApiStatus] = useState(
    Object.fromEntries(API_SOURCES.map(s => [s.id, { status: "VERIFICANDO", latencia: null }]))
  );
  const [statusOpen, setStatusOpen] = useState(false);
  const resultRef = useRef(null);

  // Simula health check
  useEffect(() => {
    const check = () => {
      const statuses = ["OK", "OK", "OK", "LENTO", "OK"];
      const latencias = [280, 920, 440, 2800, 160];
      const next = {};
      API_SOURCES.forEach((s, i) => {
        next[s.id] = { status: statuses[i], latencia: latencias[i] + Math.round(Math.random() * 100) };
      });
      setApiStatus(next);
    };
    check();
    const t = setInterval(check, 30000);
    return () => clearInterval(t);
  }, []);

  const overallStatus = () => {
    const vals = Object.values(apiStatus).map(v => v.status);
    if (vals.includes("INDISPONÍVEL")) return { label: "PARCIAL", color: "var(--red)" };
    if (vals.includes("LENTO")) return { label: "DEGRADADO", color: "var(--amber)" };
    if (vals.every(v => v === "OK")) return { label: "OPERACIONAL", color: "var(--green)" };
    return { label: "VERIFICANDO", color: "var(--muted)" };
  };

  const handleConsult = () => {
    if (cnpj.replace(/\D/g, "").length < 14) return;
    setLoading(true);
    setResult(null);
    setTimeout(() => {
      setLoading(false);
      setResult({ ...MOCK_RESULT, cnpj, consultado_em: new Date().toISOString() });
      setTimeout(() => resultRef.current?.scrollIntoView({ behavior: "smooth" }), 100);
    }, 2200);
  };

  const overall = overallStatus();

  return (
    <>
      <style>{CSS}</style>
      <div style={{ minHeight: "100vh", display: "flex", flexDirection: "column" }}>

        {/* ── Header ── */}
        <header style={{
          borderBottom: "1px solid var(--border)", padding: "0 32px",
          display: "flex", alignItems: "center", height: 56, gap: 16,
        }}>
          <span style={{ fontFamily: "var(--sans)", fontWeight: 800, fontSize: 15, color: "var(--bright)", letterSpacing: 2 }}>
            VIGIA<span style={{ color: "var(--amber)" }}>GOV</span>
          </span>
          <span style={{ flex: 1 }} />
          <button onClick={() => setStatusOpen(!statusOpen)}
            style={{
              background: "none", border: "1px solid var(--border)", cursor: "pointer",
              padding: "5px 12px", borderRadius: 3, display: "flex", alignItems: "center", gap: 7,
              color: "var(--text)", fontFamily: "var(--mono)", fontSize: 10, letterSpacing: 1,
            }}>
            <StatusDot status={overall.label === "OPERACIONAL" ? "OK" : overall.label === "DEGRADADO" ? "LENTO" : "INDISPONÍVEL"} />
            <span style={{ color: overall.color }}>{overall.label}</span>
          </button>
        </header>

        {/* ── Status panel dropdown ── */}
        {statusOpen && (
          <div style={{
            background: "var(--surface)", border: "1px solid var(--border)",
            borderTop: "none", padding: "16px 32px",
          }}>
            <div style={{ maxWidth: 700, display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 12 }}>
              {API_SOURCES.map(src => {
                const s = apiStatus[src.id];
                return (
                  <div key={src.id} style={{
                    background: "var(--bg)", border: "1px solid var(--border)",
                    borderRadius: 4, padding: "10px 14px", display: "flex", flexDirection: "column", gap: 4,
                  }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
                      <StatusDot status={s.status} />
                      <span style={{ fontFamily: "var(--sans)", fontWeight: 600, fontSize: 11, color: "var(--bright)" }}>
                        {src.nome}
                      </span>
                    </div>
                    <div style={{ fontSize: 9, color: "var(--text)", letterSpacing: 0.5 }}>{src.sub}</div>
                    <div style={{ fontSize: 9, color: s.status === "LENTO" ? "var(--amber)" : "var(--text)" }}>
                      {s.latencia ? `${s.latencia}ms` : "—"}
                      <span style={{ marginLeft: 8, color: s.status === "OK" ? "var(--green)" : s.status === "LENTO" ? "var(--amber)" : "var(--red)" }}>
                        {s.status}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* ── Main ── */}
        <main style={{ flex: 1, padding: "48px 32px", maxWidth: 800, width: "100%", margin: "0 auto" }}>

          {/* Hero */}
          <div style={{ marginBottom: 40 }}>
            <h1 style={{
              fontFamily: "var(--sans)", fontWeight: 800, fontSize: 28,
              color: "var(--bright)", letterSpacing: -0.5, lineHeight: 1.2, marginBottom: 10,
            }}>
              Inteligência sobre<br />
              <span style={{ color: "var(--amber)" }}>Fornecedores Públicos</span>
            </h1>
            <p style={{ fontSize: 12, color: "var(--text)", lineHeight: 1.7, maxWidth: 480 }}>
              Cruza dados da Receita Federal, Portal da Transparência e listas de sanções.
              Gera score de risco e narrativa interpretativa em segundos.
            </p>
          </div>

          {/* Input */}
          <div style={{
            background: "var(--surface)", border: "1px solid var(--border)",
            borderRadius: 6, padding: "24px 28px", marginBottom: 32,
          }}>
            <label style={{ display: "block", fontSize: 10, letterSpacing: 2, color: "var(--text)", marginBottom: 10, textTransform: "uppercase" }}>
              CNPJ
            </label>
            <div style={{ display: "flex", gap: 12 }}>
              <input
                value={cnpj}
                onChange={e => setCnpj(maskCNPJ(e.target.value))}
                onKeyDown={e => e.key === "Enter" && handleConsult()}
                placeholder="00.000.000/0000-00"
                maxLength={18}
                style={{
                  flex: 1, background: "var(--bg)", border: "1px solid var(--border)",
                  borderRadius: 4, padding: "12px 16px", color: "var(--bright)",
                  fontFamily: "var(--mono)", fontSize: 16, letterSpacing: 2, outline: "none",
                }}
              />
              <button onClick={handleConsult} disabled={loading || cnpj.replace(/\D/g, "").length < 14}
                style={{
                  background: loading ? "var(--muted)" : "var(--amber)",
                  color: loading ? "var(--text)" : "#080b0f",
                  border: "none", borderRadius: 4, padding: "12px 24px",
                  fontFamily: "var(--sans)", fontWeight: 700, fontSize: 12,
                  letterSpacing: 1.5, cursor: loading ? "not-allowed" : "pointer",
                  transition: "all .2s", minWidth: 120,
                }}>
                {loading ? "CONSULTANDO..." : "CONSULTAR"}
              </button>
            </div>
          </div>

          {/* Loading */}
          {loading && (
            <div style={{ textAlign: "center", padding: "40px 0" }}>
              <div style={{
                width: 32, height: 32, border: "2px solid var(--border)",
                borderTop: "2px solid var(--amber)", borderRadius: "50%",
                animation: "spin 0.8s linear infinite", margin: "0 auto 16px",
              }} />
              <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
              <p style={{ fontSize: 10, letterSpacing: 2, color: "var(--text)" }}>CONSULTANDO FONTES GOVERNAMENTAIS...</p>
            </div>
          )}

          {/* Result */}
          {result && (
            <div ref={resultRef} style={{
              background: "var(--surface)", border: "1px solid var(--border)",
              borderRadius: 6, padding: "28px",
              animation: "fadeIn .4s ease",
            }}>
              <style>{`@keyframes fadeIn { from { opacity:0; transform:translateY(8px) } to { opacity:1; transform:translateY(0) } }`}</style>

              {/* Header do resultado */}
              <div style={{ display: "flex", alignItems: "flex-start", gap: 24, marginBottom: 24, flexWrap: "wrap" }}>
                <div style={{ flex: 1, minWidth: 200 }}>
                  <div style={{ fontSize: 9, letterSpacing: 2, color: "var(--text)", marginBottom: 4 }}>EMPRESA</div>
                  <div style={{ fontFamily: "var(--sans)", fontWeight: 700, fontSize: 16, color: "var(--bright)", marginBottom: 4 }}>
                    {result.razao_social}
                  </div>
                  <div style={{ fontSize: 11, color: "var(--text)", marginBottom: 8, letterSpacing: 1 }}>{result.cnpj}</div>
                  <span style={{
                    fontSize: 9, letterSpacing: 1.5, padding: "3px 10px", borderRadius: 2,
                    background: result.situacao_cadastral === "ATIVA" ? "rgba(45,202,115,.15)" : "rgba(232,69,90,.15)",
                    color: result.situacao_cadastral === "ATIVA" ? "var(--green)" : "var(--red)",
                  }}>
                    {result.situacao_cadastral}
                  </span>
                </div>
                <ScoreGauge value={result.score.valor} label={result.score.classificacao} />
              </div>

              {/* Narrativa */}
              <div style={{
                background: "var(--bg)", border: "1px solid var(--border)",
                borderLeft: "3px solid var(--amber)", borderRadius: "0 4px 4px 0",
                padding: "16px 20px", marginBottom: 20,
              }}>
                <div style={{ fontSize: 9, letterSpacing: 2, color: "var(--amber)", marginBottom: 8 }}>ANÁLISE LLM</div>
                <p style={{ fontSize: 12, lineHeight: 1.8, color: "var(--bright)" }}>{result.narrativa}</p>
              </div>

              {/* Exportação */}
              <div style={{ display: "flex", gap: 8, marginBottom: 20 }}>
                {[
                  { label: "↓ JSON", fn: () => exportJSON(result) },
                  { label: "↓ TXT", fn: () => exportText(result) },
                ].map(btn => (
                  <button key={btn.label} onClick={btn.fn}
                    style={{
                      background: "none", border: "1px solid var(--border)",
                      color: "var(--text)", borderRadius: 3, padding: "6px 16px",
                      fontFamily: "var(--mono)", fontSize: 10, letterSpacing: 1,
                      cursor: "pointer", transition: "border-color .2s",
                    }}
                    onMouseEnter={e => e.target.style.borderColor = "var(--amber)"}
                    onMouseLeave={e => e.target.style.borderColor = "var(--border)"}>
                    {btn.label}
                  </button>
                ))}
                <span style={{ marginLeft: "auto", fontSize: 9, color: "var(--text)", alignSelf: "center" }}>
                  {result.cache ? "📦 cache" : "🔴 live"} · {new Date(result.consultado_em).toLocaleString("pt-BR")}
                </span>
              </div>

              {/* Seções colapsáveis */}
              <Section title="Dados Cadastrais" defaultOpen>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px 24px", fontSize: 11 }}>
                  {[
                    ["Abertura", fmtDate(result.data_abertura)],
                    ["Porte", result.porte],
                    ["Capital Social", fmtBRL(result.capital_social)],
                    ["Nome Fantasia", result.nome_fantasia],
                  ].map(([k, v]) => (
                    <div key={k}>
                      <span style={{ color: "var(--text)", fontSize: 9, letterSpacing: 1 }}>{k.toUpperCase()} </span>
                      <span style={{ color: "var(--bright)" }}>{v}</span>
                    </div>
                  ))}
                </div>
                <div style={{ marginTop: 14 }}>
                  <div style={{ fontSize: 9, letterSpacing: 1.5, color: "var(--text)", marginBottom: 8 }}>QUADRO SOCIETÁRIO</div>
                  {result.socios.map((s, i) => (
                    <div key={i} style={{
                      fontSize: 11, display: "flex", justifyContent: "space-between",
                      padding: "6px 0", borderBottom: "1px solid var(--border)",
                    }}>
                      <span style={{ color: "var(--bright)" }}>{s.nome}</span>
                      <span style={{ color: "var(--text)", fontSize: 10 }}>{s.qualificacao}</span>
                    </div>
                  ))}
                </div>
              </Section>

              <Section title="Contratos Públicos" badge={result.contratos_publicos.total_contratos} badgeColor="rgba(74,158,255,.25)">
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px 24px", fontSize: 11, marginBottom: 12 }}>
                  {[
                    ["Total de Contratos", result.contratos_publicos.total_contratos],
                    ["Valor Total", fmtBRL(result.contratos_publicos.valor_total_brl)],
                    ["Último Contrato", fmtDate(result.contratos_publicos.ultimo_contrato)],
                  ].map(([k, v]) => (
                    <div key={k}>
                      <span style={{ color: "var(--text)", fontSize: 9, letterSpacing: 1 }}>{k.toUpperCase()} </span>
                      <span style={{ color: "var(--bright)" }}>{v}</span>
                    </div>
                  ))}
                </div>
                <div style={{ fontSize: 9, letterSpacing: 1.5, color: "var(--text)", marginBottom: 6 }}>ÓRGÃOS CONTRATANTES</div>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                  {result.contratos_publicos.orgaos_contratantes.map(o => (
                    <span key={o} style={{
                      fontSize: 10, padding: "3px 10px", borderRadius: 2,
                      background: "rgba(74,158,255,.1)", color: "var(--blue)", letterSpacing: 0.5,
                    }}>{o}</span>
                  ))}
                </div>
              </Section>

              <Section title="Sanções"
                badge={result.sancoes.cnep || result.sancoes.ceis ? "ATIVO" : "LIMPO"}
                badgeColor={result.sancoes.cnep || result.sancoes.ceis ? "rgba(232,69,90,.25)" : "rgba(45,202,115,.15)"}>
                <div style={{ display: "flex", gap: 12, marginBottom: 12 }}>
                  {[
                    { label: "CEIS", val: result.sancoes.ceis },
                    { label: "CNEP", val: result.sancoes.cnep },
                  ].map(s => (
                    <div key={s.label} style={{
                      flex: 1, padding: "12px 16px", borderRadius: 4,
                      background: s.val ? "rgba(232,69,90,.1)" : "rgba(45,202,115,.08)",
                      border: `1px solid ${s.val ? "rgba(232,69,90,.3)" : "rgba(45,202,115,.2)"}`,
                      display: "flex", alignItems: "center", gap: 8,
                    }}>
                      <span style={{ fontSize: 14 }}>{s.val ? "⚠" : "✓"}</span>
                      <div>
                        <div style={{ fontSize: 9, letterSpacing: 1.5, color: "var(--text)" }}>{s.label}</div>
                        <div style={{ fontSize: 11, color: s.val ? "var(--red)" : "var(--green)", fontWeight: 500 }}>
                          {s.val ? "REGISTRADO" : "LIMPO"}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
                {result.sancoes.detalhes.map((d, i) => (
                  <div key={i} style={{
                    fontSize: 11, padding: "10px 14px", borderRadius: 4,
                    background: "rgba(232,69,90,.07)", border: "1px solid rgba(232,69,90,.2)",
                  }}>
                    <span style={{ color: "var(--red)", fontWeight: 500 }}>{d.lista}</span>
                    <span style={{ color: "var(--text)", marginLeft: 8 }}>· {d.motivo}</span>
                    <span style={{ color: "var(--text)", marginLeft: 8, fontSize: 10 }}>· {d.orgao} · {fmtDate(d.data)}</span>
                    <span style={{ color: "var(--text)", marginLeft: 8, fontSize: 10 }}>· vigência até {fmtDate(d.vigencia)}</span>
                  </div>
                ))}
              </Section>

              <Section title="Fatores do Score">
                {result.score.fatores.map((f, i) => (
                  <div key={i} style={{
                    display: "flex", alignItems: "center", gap: 10,
                    padding: "8px 0", borderBottom: "1px solid var(--border)", fontSize: 11,
                  }}>
                    <span style={{ fontSize: 12 }}>{f.impacto === "positivo" ? "↑" : "↓"}</span>
                    <span style={{ flex: 1, color: "var(--bright)" }}>{f.fator}</span>
                    <span style={{
                      fontSize: 9, padding: "2px 8px", borderRadius: 2, letterSpacing: 1,
                      background: f.peso === "alto" ? "rgba(245,166,35,.15)" : "var(--muted)",
                      color: f.peso === "alto" ? "var(--amber)" : "var(--text)",
                    }}>{f.peso.toUpperCase()}</span>
                    <span style={{ color: f.impacto === "positivo" ? "var(--green)" : "var(--red)", fontSize: 10 }}>
                      {f.impacto}
                    </span>
                  </div>
                ))}
              </Section>

              <div style={{ marginTop: 16, fontSize: 9, color: "var(--muted)", letterSpacing: 1 }}>
                Fontes: {result.fontes.join(" · ")}
              </div>
            </div>
          )}
        </main>

        {/* ── Footer ── */}
        <footer style={{
          borderTop: "1px solid var(--border)", padding: "14px 32px",
          display: "flex", alignItems: "center", gap: 12,
          fontSize: 9, color: "var(--muted)", letterSpacing: 1,
        }}>
          <span>VIGIAGOV © 2026 · TECKER CONSULTING</span>
          <span style={{ flex: 1 }} />
          <span>MVP v0.1 · USO INTERNO</span>
        </footer>
      </div>
    </>
  );
}
