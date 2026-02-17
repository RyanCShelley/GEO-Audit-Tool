import { useState } from "react";
import { useNavigate, useParams, Link } from "react-router-dom";
import { startAudit, startSeedCrawl } from "../api/client";

type Mode = "urls" | "seed";

export default function AuditPage() {
  const navigate = useNavigate();
  const { projectId } = useParams();
  const [mode, setMode] = useState<Mode>("urls");
  const [urlInput, setUrlInput] = useState("");
  const [seedUrl, setSeedUrl] = useState("");
  const [candidateUrls, setCandidateUrls] = useState<string[]>([]);
  const [selectedCandidates, setSelectedCandidates] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const reportBase = projectId ? `/project/${projectId}/report` : "/report";

  async function handleSeedCrawl() {
    if (!seedUrl.trim()) return;
    setLoading(true);
    setError("");
    try {
      const res = await startSeedCrawl(seedUrl.trim());
      setCandidateUrls(res.candidate_urls);
      setSelectedCandidates(new Set(res.candidate_urls.slice(0, 5)));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Seed crawl failed");
    } finally {
      setLoading(false);
    }
  }

  async function handleStartAudit() {
    setLoading(true);
    setError("");
    try {
      let urls: string[];
      if (mode === "seed" && candidateUrls.length > 0) {
        urls = Array.from(selectedCandidates);
      } else {
        urls = urlInput
          .split("\n")
          .map((u) => u.trim())
          .filter(Boolean);
      }

      if (urls.length === 0) {
        setError("Please enter at least one URL.");
        setLoading(false);
        return;
      }

      const res = await startAudit(urls, undefined, projectId);
      if ("job_id" in res) {
        navigate(`${reportBase}/${res.job_id}`);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to start audit");
    } finally {
      setLoading(false);
    }
  }

  function toggleCandidate(url: string) {
    setSelectedCandidates((prev) => {
      const next = new Set(prev);
      if (next.has(url)) next.delete(url);
      else next.add(url);
      return next;
    });
  }

  return (
    <div className="audit-page">
      {projectId && (
        <div className="breadcrumbs">
          <Link to="/">Projects</Link> / <Link to={`/project/${projectId}`}>Project</Link> / Audit
        </div>
      )}
      <h1>GEO Audit Tool</h1>
      <p>Analyze your website's structured data for GEO (Generative Engine Optimization) visibility.</p>

      <div className="mode-tabs">
        <button
          className={`mode-tab ${mode === "urls" ? "mode-tab--active" : ""}`}
          onClick={() => setMode("urls")}
        >
          Direct URLs
        </button>
        <button
          className={`mode-tab ${mode === "seed" ? "mode-tab--active" : ""}`}
          onClick={() => setMode("seed")}
        >
          Crawl from Seed
        </button>
      </div>

      {mode === "urls" ? (
        <div className="input-section">
          <label htmlFor="urls">Enter URLs (one per line):</label>
          <textarea
            id="urls"
            rows={6}
            value={urlInput}
            onChange={(e) => setUrlInput(e.target.value)}
            placeholder={"https://example.com/\nhttps://example.com/services/\nhttps://example.com/about/"}
          />
          <button
            className="btn btn--primary"
            onClick={handleStartAudit}
            disabled={loading || !urlInput.trim()}
          >
            {loading ? "Starting..." : "Start Audit"}
          </button>
        </div>
      ) : (
        <div className="input-section">
          <label htmlFor="seed">Seed URL:</label>
          <div className="seed-row">
            <input
              id="seed"
              type="url"
              value={seedUrl}
              onChange={(e) => setSeedUrl(e.target.value)}
              placeholder="https://example.com/"
            />
            <button
              className="btn"
              onClick={handleSeedCrawl}
              disabled={loading || !seedUrl.trim()}
            >
              {loading ? "Crawling..." : "Discover URLs"}
            </button>
          </div>

          {candidateUrls.length > 0 && (
            <div className="candidate-urls">
              <h3>Select URLs to audit ({selectedCandidates.size} selected):</h3>
              <div className="candidate-list">
                {candidateUrls.map((u) => (
                  <label key={u} className="candidate-option">
                    <input
                      type="checkbox"
                      checked={selectedCandidates.has(u)}
                      onChange={() => toggleCandidate(u)}
                    />
                    <span>{u}</span>
                  </label>
                ))}
              </div>
              <button
                className="btn btn--primary"
                onClick={handleStartAudit}
                disabled={loading || selectedCandidates.size === 0}
              >
                {loading ? "Starting..." : `Audit ${selectedCandidates.size} URLs`}
              </button>
            </div>
          )}
        </div>
      )}

      {error && <p className="error">{error}</p>}
    </div>
  );
}
