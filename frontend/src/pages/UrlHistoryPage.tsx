import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { getUrlHistory, type UrlHistoryResponse } from "../api/client";

export default function UrlHistoryPage() {
  const { projectId, urlId } = useParams();
  const [history, setHistory] = useState<UrlHistoryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [expanded, setExpanded] = useState<string | null>(null);

  useEffect(() => {
    if (projectId && urlId) {
      loadHistory();
    }
  }, [projectId, urlId]);

  async function loadHistory() {
    try {
      const data = await getUrlHistory(projectId!, urlId!);
      setHistory(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load history");
    } finally {
      setLoading(false);
    }
  }

  function toggleExpand(key: string) {
    setExpanded((prev) => (prev === key ? null : key));
  }

  if (loading) return <div className="url-history-page"><p>Loading...</p></div>;
  if (error) return <div className="url-history-page"><p className="error">{error}</p></div>;
  if (!history) return <div className="url-history-page"><p>URL not found.</p></div>;

  return (
    <div className="url-history-page">
      <div className="breadcrumbs">
        <Link to="/">Projects</Link> / <Link to={`/project/${projectId}`}>Project</Link> / URL History
      </div>

      <h1>Audit History</h1>
      <p className="text-muted url-history-url">{history.url}</p>

      {history.entries.length === 0 ? (
        <p className="text-muted">No audit results for this URL yet.</p>
      ) : (
        <div className="history-entries">
          {history.entries.map((entry, i) => {
            const key = `${entry.job_id}-${i}`;
            const isExpanded = expanded === key;
            const r = entry.data;
            return (
              <div key={key} className="history-entry">
                <button
                  className="history-entry__header"
                  onClick={() => toggleExpand(key)}
                >
                  <div className="history-entry__info">
                    <span className="history-entry__date">
                      {new Date(entry.created_at).toLocaleString()}
                    </span>
                    <span className="history-entry__intent">{r.page_intent || "—"}</span>
                    <span className="history-entry__meta">
                      {r.json_ld_corrections?.length ?? 0} corrections
                      {" · "}
                      {r.used_qids?.length ?? 0} QIDs
                    </span>
                  </div>
                  <span className="history-entry__toggle">{isExpanded ? "▲" : "▼"}</span>
                </button>
                {isExpanded && (
                  <div className="history-entry__body">
                    <div className="report-section">
                      <h3>Page Intent</h3>
                      <p>{r.page_intent || "—"}</p>
                    </div>
                    <div className="report-section">
                      <h3>Visibility Diagnosis</h3>
                      <pre className="report-pre">{r.visibility_diagnosis || "—"}</pre>
                    </div>
                    <div className="report-section">
                      <h3>Fix Plan</h3>
                      <pre className="report-pre">{r.fix_plan || "—"}</pre>
                    </div>
                    {r.json_ld != null && (
                      <div className="report-section">
                        <h3>JSON-LD</h3>
                        <pre className="code-block">{JSON.stringify(r.json_ld, null, 2)}</pre>
                      </div>
                    )}
                    {r.used_qids?.length > 0 && (
                      <div className="report-section">
                        <h3>Approved QIDs</h3>
                        <ul>
                          {r.used_qids.map((q) => (
                            <li key={q.qid}><strong>{q.name}</strong>: {q.qid}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                    <Link
                      to={`/project/${projectId}/report/${entry.job_id}`}
                      className="btn btn--small"
                    >
                      View Full Report
                    </Link>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
