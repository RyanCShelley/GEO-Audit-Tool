import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { listProjects, createProject, type Project } from "../api/client";

export default function ProjectListPage() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [loading, setLoading] = useState(true);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    loadProjects();
  }, []);

  async function loadProjects() {
    try {
      const data = await listProjects();
      setProjects(data);
      setError("");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load projects");
    } finally {
      setLoading(false);
    }
  }

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault();
    if (!name.trim()) return;
    setCreating(true);
    setError("");
    try {
      await createProject(name.trim(), description.trim());
      setName("");
      setDescription("");
      // Reload the full list from server to get url_count / last_audit
      await loadProjects();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create project");
    } finally {
      setCreating(false);
    }
  }

  return (
    <div className="project-list-page">
      <h1>Projects</h1>
      <p className="text-muted">
        Group URLs by client or site, track audit history, and auto-reuse approved QIDs.
      </p>

      <form className="project-create-form" onSubmit={handleCreate}>
        <input
          type="text"
          placeholder="Project name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          required
        />
        <input
          type="text"
          placeholder="Description (optional)"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
        />
        <button className="btn btn--primary" type="submit" disabled={creating || !name.trim()}>
          {creating ? "Creating..." : "Create Project"}
        </button>
      </form>

      {error && <p className="error">{error}</p>}

      {loading ? (
        <p>Loading projects...</p>
      ) : projects.length === 0 ? (
        <p className="text-muted">No projects yet. Create one above or <Link to="/audit">run a quick audit</Link>.</p>
      ) : (
        <div className="project-grid">
          {projects.map((p) => (
            <Link key={p.id} to={`/project/${p.id}`} className="project-card">
              <h3>{p.name}</h3>
              {p.description && <p className="project-card__desc">{p.description}</p>}
              <div className="project-card__meta">
                <span>{p.url_count ?? 0} URLs</span>
                <span>{p.last_audit ? `Last audit: ${new Date(p.last_audit).toLocaleDateString()}` : "No audits yet"}</span>
              </div>
            </Link>
          ))}
        </div>
      )}

      <div className="quick-audit-link">
        <Link to="/audit" className="btn">Quick Audit (no project)</Link>
      </div>
    </div>
  );
}
