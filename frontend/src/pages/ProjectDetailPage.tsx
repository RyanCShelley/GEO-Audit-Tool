import { useEffect, useState } from "react";
import { useParams, useNavigate, Link } from "react-router-dom";
import {
  getProject,
  updateProject,
  deleteProject,
  addProjectUrls,
  removeProjectUrl,
  startAudit,
  type ProjectDetail,
} from "../api/client";

export default function ProjectDetailPage() {
  const { projectId } = useParams();
  const navigate = useNavigate();
  const [project, setProject] = useState<ProjectDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // Edit state
  const [editing, setEditing] = useState(false);
  const [editName, setEditName] = useState("");
  const [editDesc, setEditDesc] = useState("");

  // URL management
  const [urlInput, setUrlInput] = useState("");
  const [addingUrls, setAddingUrls] = useState(false);
  const [selectedUrls, setSelectedUrls] = useState<Set<string>>(new Set());

  // Audit
  const [auditing, setAuditing] = useState(false);

  useEffect(() => {
    if (projectId) loadProject();
  }, [projectId]);

  async function loadProject() {
    try {
      const data = await getProject(projectId!);
      setProject(data);
      setEditName(data.name);
      setEditDesc(data.description);
      setSelectedUrls(new Set(data.urls.map((u) => u.id)));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load project");
    } finally {
      setLoading(false);
    }
  }

  async function handleSave() {
    try {
      await updateProject(projectId!, { name: editName, description: editDesc });
      setProject((p) => p ? { ...p, name: editName, description: editDesc } : p);
      setEditing(false);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to update project");
    }
  }

  async function handleDelete() {
    if (!confirm("Delete this project and all its data?")) return;
    try {
      await deleteProject(projectId!);
      navigate("/");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete project");
    }
  }

  async function handleAddUrls() {
    const urls = urlInput
      .split("\n")
      .map((u) => u.trim())
      .filter(Boolean);
    if (urls.length === 0) return;
    setAddingUrls(true);
    try {
      const { added } = await addProjectUrls(projectId!, urls);
      setProject((p) => {
        if (!p) return p;
        const existingIds = new Set(p.urls.map((u) => u.id));
        const newUrls = added.filter((u) => !existingIds.has(u.id));
        return { ...p, urls: [...p.urls, ...newUrls] };
      });
      setUrlInput("");
      // Auto-select new URLs
      setSelectedUrls((prev) => {
        const next = new Set(prev);
        added.forEach((u) => next.add(u.id));
        return next;
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to add URLs");
    } finally {
      setAddingUrls(false);
    }
  }

  async function handleRemoveUrl(urlId: string) {
    try {
      await removeProjectUrl(projectId!, urlId);
      setProject((p) => {
        if (!p) return p;
        return { ...p, urls: p.urls.filter((u) => u.id !== urlId) };
      });
      setSelectedUrls((prev) => {
        const next = new Set(prev);
        next.delete(urlId);
        return next;
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to remove URL");
    }
  }

  function toggleUrl(urlId: string) {
    setSelectedUrls((prev) => {
      const next = new Set(prev);
      if (next.has(urlId)) next.delete(urlId);
      else next.add(urlId);
      return next;
    });
  }

  function toggleAll() {
    if (!project) return;
    if (selectedUrls.size === project.urls.length) {
      setSelectedUrls(new Set());
    } else {
      setSelectedUrls(new Set(project.urls.map((u) => u.id)));
    }
  }

  async function handleRunAudit() {
    if (!project || selectedUrls.size === 0) return;
    setAuditing(true);
    setError("");
    try {
      const urls = project.urls
        .filter((u) => selectedUrls.has(u.id))
        .map((u) => u.url);
      const res = await startAudit(urls, undefined, projectId);
      if ("job_id" in res) {
        navigate(`/project/${projectId}/report/${res.job_id}`);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to start audit");
    } finally {
      setAuditing(false);
    }
  }

  if (loading) return <div className="project-detail-page"><p>Loading...</p></div>;
  if (error && !project) return <div className="project-detail-page"><p className="error">{error}</p></div>;
  if (!project) return <div className="project-detail-page"><p>Project not found.</p></div>;

  return (
    <div className="project-detail-page">
      <div className="breadcrumbs">
        <Link to="/">Projects</Link> / {project.name}
      </div>

      {/* Header */}
      <div className="project-header">
        {editing ? (
          <div className="project-edit-form">
            <input
              type="text"
              value={editName}
              onChange={(e) => setEditName(e.target.value)}
              placeholder="Project name"
            />
            <input
              type="text"
              value={editDesc}
              onChange={(e) => setEditDesc(e.target.value)}
              placeholder="Description"
            />
            <div className="project-edit-actions">
              <button className="btn btn--primary btn--small" onClick={handleSave}>Save</button>
              <button className="btn btn--small" onClick={() => setEditing(false)}>Cancel</button>
            </div>
          </div>
        ) : (
          <>
            <div>
              <h1>{project.name}</h1>
              {project.description && <p className="text-muted">{project.description}</p>}
            </div>
            <div className="project-header-actions">
              <button className="btn btn--small" onClick={() => setEditing(true)}>Edit</button>
              <button className="btn btn--small btn--danger" onClick={handleDelete}>Delete</button>
            </div>
          </>
        )}
      </div>

      {error && <p className="error">{error}</p>}

      {/* URL Management */}
      <section className="project-section">
        <h2>URLs ({project.urls.length})</h2>
        <div className="url-add-form">
          <textarea
            rows={3}
            value={urlInput}
            onChange={(e) => setUrlInput(e.target.value)}
            placeholder="Add URLs (one per line)"
          />
          <button
            className="btn btn--primary"
            onClick={handleAddUrls}
            disabled={addingUrls || !urlInput.trim()}
          >
            {addingUrls ? "Adding..." : "Add URLs"}
          </button>
        </div>

        {project.urls.length > 0 && (
          <div className="url-list">
            <div className="url-list__header">
              <label className="url-list__select-all">
                <input
                  type="checkbox"
                  checked={selectedUrls.size === project.urls.length}
                  onChange={toggleAll}
                />
                <span>Select all</span>
              </label>
              <button
                className="btn btn--primary"
                onClick={handleRunAudit}
                disabled={auditing || selectedUrls.size === 0}
              >
                {auditing ? "Starting..." : `Run Audit (${selectedUrls.size} URLs)`}
              </button>
            </div>
            {project.urls.map((u) => (
              <div key={u.id} className="url-list__item">
                <label className="url-list__checkbox">
                  <input
                    type="checkbox"
                    checked={selectedUrls.has(u.id)}
                    onChange={() => toggleUrl(u.id)}
                  />
                  <span className="url-list__url">{u.url}</span>
                </label>
                <div className="url-list__actions">
                  <Link to={`/project/${projectId}/url/${u.id}/history`} className="btn btn--small">
                    History
                  </Link>
                  <button className="btn btn--small btn--danger" onClick={() => handleRemoveUrl(u.id)}>
                    Remove
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      {/* Audit History */}
      <section className="project-section">
        <h2>Audit History</h2>
        {project.recent_jobs.length === 0 ? (
          <p className="text-muted">No audits yet. Add URLs above and run your first audit.</p>
        ) : (
          <table className="history-table">
            <thead>
              <tr>
                <th>Date</th>
                <th>Status</th>
                <th>URLs</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {project.recent_jobs.map((j) => (
                <tr key={j.id}>
                  <td>{new Date(j.created_at).toLocaleString()}</td>
                  <td>
                    <span className={`status-badge status-badge--${j.status}`}>{j.status}</span>
                  </td>
                  <td>{j.url_count ?? j.progress_total ?? "—"}</td>
                  <td>
                    <Link to={`/project/${projectId}/report/${j.id}`} className="btn btn--small">
                      View Report
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <div className="project-footer">
        <Link to={`/project/${projectId}/audit`} className="btn">
          Advanced Audit (seed crawl)
        </Link>
      </div>
    </div>
  );
}
