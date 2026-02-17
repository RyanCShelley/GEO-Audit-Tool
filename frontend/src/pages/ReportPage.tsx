import { useEffect, useState, useRef } from "react";
import { useParams, Link } from "react-router-dom";
import {
  getAuditStatus,
  regenerateReport,
  type JobResponse,
  type AuditResult,
} from "../api/client";
import QidReview from "../components/QidReview";

type Tab = "report" | "jsonld" | "flattened" | "practices" | "qids";

export default function ReportPage() {
  const { jobId, projectId } = useParams();
  const [job, setJob] = useState<JobResponse | null>(null);
  const [error, setError] = useState("");
  const [activeResult, setActiveResult] = useState(0);
  const [activeTab, setActiveTab] = useState<Tab>("report");
  const [regenerating, setRegenerating] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | undefined>(undefined);

  useEffect(() => {
    if (!jobId) return;

    async function poll() {
      try {
        const data = await getAuditStatus(jobId!);
        setJob(data);
        if (data.status === "completed" || data.status === "failed") {
          clearInterval(pollRef.current);
        }
      } catch (e) {
        setError(e instanceof Error ? e.message : "Failed to load job");
        clearInterval(pollRef.current);
      }
    }

    poll();
    pollRef.current = setInterval(poll, 3000);
    return () => clearInterval(pollRef.current);
  }, [jobId]);

  async function handleApproveQids(
    result: AuditResult,
    approved: { name: string; qid: string }[]
  ) {
    if (!jobId) return;
    setRegenerating(true);
    try {
      const updated = await regenerateReport(jobId, result.url, approved, projectId);
      // Replace the result in the job
      setJob((prev) => {
        if (!prev) return prev;
        const results = [...prev.results];
        results[activeResult] = updated;
        return { ...prev, results };
      });
      setActiveTab("jsonld");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Regeneration failed");
    } finally {
      setRegenerating(false);
    }
  }

  if (error) {
    return <div className="report-page"><p className="error">{error}</p></div>;
  }

  if (!job) {
    return <div className="report-page"><p>Loading...</p></div>;
  }

  const isRunning = job.status === "running" || job.status === "pending";
  const result: AuditResult | undefined = job.results[activeResult];

  return (
    <div className="report-page">
      {projectId && (
        <div className="breadcrumbs">
          <Link to="/">Projects</Link> / <Link to={`/project/${projectId}`}>Project</Link> / Report
        </div>
      )}
      <h1>Audit Report</h1>
      {job.user_name && (
        <p className="text-muted">Run by: {job.user_name}</p>
      )}

      {/* Progress bar */}
      {isRunning && (
        <div className="progress-section">
          <div className="progress-bar">
            <div
              className="progress-bar__fill"
              style={{ width: `${(job.progress.current / job.progress.total) * 100}%` }}
            />
          </div>
          <p>
            Analyzing {job.progress.current} of {job.progress.total}
            {job.progress.current_url && <span>: {job.progress.current_url}</span>}
          </p>
        </div>
      )}

      {/* Errors */}
      {job.errors.length > 0 && (
        <div className="errors-section">
          <h3>Errors</h3>
          {job.errors.map((err, i) => (
            <p key={i} className="error">
              [{err.stage}] {err.url}: {err.message}
            </p>
          ))}
        </div>
      )}

      {/* URL selector (if multiple results) */}
      {job.results.length > 1 && (
        <div className="url-selector">
          {job.results.map((r, i) => (
            <button
              key={r.url}
              className={`url-tab ${i === activeResult ? "url-tab--active" : ""}`}
              onClick={() => { setActiveResult(i); setActiveTab("report"); }}
            >
              {new URL(r.url).pathname || "/"}
            </button>
          ))}
        </div>
      )}

      {result && (
        <>
          {/* Tab bar */}
          <div className="tab-bar">
            {(["report", "jsonld", "flattened", "practices", "qids"] as Tab[]).map((tab) => (
              <button
                key={tab}
                className={`tab ${activeTab === tab ? "tab--active" : ""}`}
                onClick={() => setActiveTab(tab)}
              >
                {tab === "report" && "Report"}
                {tab === "jsonld" && "JSON-LD"}
                {tab === "flattened" && "Flattened Schema"}
                {tab === "practices" && "Best Practices"}
                {tab === "qids" && `QIDs${result.used_qids.length ? ` (${result.used_qids.length})` : ""}`}
              </button>
            ))}
          </div>

          {/* Tab content */}
          <div className="tab-content">
            {activeTab === "report" && (
              <div>
                <div className="report-section">
                  <h3>Page Intent</h3>
                  <p>{result.page_intent || "—"}</p>
                </div>
                <div className="report-section">
                  <h3>Visibility Diagnosis</h3>
                  <pre className="report-pre">{result.visibility_diagnosis || "—"}</pre>
                </div>
                <div className="report-section">
                  <h3>Fix Plan</h3>
                  <pre className="report-pre">{result.fix_plan || "—"}</pre>
                </div>
                {!result.rendered_html_available && (
                  <p className="warning">
                    Note: Playwright rendering was unavailable. Results are based on server HTML only.
                  </p>
                )}
              </div>
            )}

            {activeTab === "jsonld" && (
              <div>
                <h3>Corrected JSON-LD</h3>
                {result.json_ld ? (
                  <>
                    <pre className="code-block">
                      {JSON.stringify(result.json_ld, null, 2)}
                    </pre>
                    <button
                      className="btn btn--small"
                      onClick={() => navigator.clipboard.writeText(JSON.stringify(result.json_ld, null, 2))}
                    >
                      Copy JSON-LD
                    </button>
                  </>
                ) : (
                  <p>No JSON-LD generated.</p>
                )}
                {result.json_ld_corrections.length > 0 && (
                  <div className="corrections">
                    <h4>Post-processing corrections applied:</h4>
                    <ul>
                      {result.json_ld_corrections.map((c, i) => (
                        <li key={i}>
                          <strong>{c.transform}</strong>: {c.detail}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}

            {activeTab === "flattened" && (
              <div>
                <h3>Flattened Schema (for Vector Search)</h3>
                {result.flattened_schema ? (
                  <>
                    <pre className="code-block code-block--prose">
                      {result.flattened_schema}
                    </pre>
                    <button
                      className="btn btn--small"
                      onClick={() => navigator.clipboard.writeText(result.flattened_schema)}
                    >
                      Copy Flattened Text
                    </button>
                  </>
                ) : (
                  <p>No flattened schema available.</p>
                )}
              </div>
            )}

            {activeTab === "practices" && (
              <div>
                <h3>Implementation Best Practices</h3>
                <pre className="report-pre">{result.best_practices}</pre>
              </div>
            )}

            {activeTab === "qids" && (
              <div>
                {result.used_qids.length > 0 && (
                  <div className="used-qids">
                    <h3>Approved QIDs in use:</h3>
                    <ul>
                      {result.used_qids.map((q) => (
                        <li key={q.qid}>
                          <strong>{q.name}</strong>: {q.qid}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                {result.suggested_qids.length > 0 && (
                  <QidReview
                    suggestedQids={result.suggested_qids}
                    onApprove={(approved) => handleApproveQids(result, approved)}
                    loading={regenerating}
                  />
                )}
                {result.suggested_qids.length === 0 && result.used_qids.length === 0 && (
                  <p>No QID suggestions available for this page.</p>
                )}
              </div>
            )}
          </div>
        </>
      )}

      {!isRunning && job.results.length === 0 && (
        <p>No results were produced. Check errors above.</p>
      )}
    </div>
  );
}
