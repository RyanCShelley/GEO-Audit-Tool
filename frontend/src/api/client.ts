const API_BASE = import.meta.env.VITE_API_URL || "";
const TOKEN_KEY = "geo_audit_token";

function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string): void {
  localStorage.setItem(TOKEN_KEY, token);
}

export function clearToken(): void {
  localStorage.removeItem(TOKEN_KEY);
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const url = `${API_BASE}${path}`;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options?.headers as Record<string, string>),
  };

  // Inject auth token
  const token = getToken();
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(url, { ...options, headers });

  // On 401, clear token and redirect to login
  if (res.status === 401) {
    clearToken();
    if (window.location.pathname !== "/login") {
      window.location.href = "/login";
    }
    throw new Error("Session expired. Please log in again.");
  }

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.detail || `Request failed: ${res.status}`);
  }
  return res.json();
}

// --- Auth Types ---

export interface AuthUser {
  id: string;
  email: string;
  name: string;
  role: "admin" | "member";
  team_id: string;
  team_name?: string;
}

export interface AuthResponse {
  token: string;
  user: AuthUser;
}

export interface TeamMember {
  id: string;
  email: string;
  name: string;
  role: string;
  created_at: string;
}

export interface InviteResponse {
  invite_token: string;
  email: string;
}

// --- Auth API functions ---

export async function loginUser(
  email: string,
  password: string
): Promise<AuthResponse> {
  return request("/api/auth/login", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
}

export async function registerUser(
  email: string,
  password: string,
  name: string,
  teamName?: string,
  inviteToken?: string
): Promise<AuthResponse> {
  return request("/api/auth/register", {
    method: "POST",
    body: JSON.stringify({
      email,
      password,
      name,
      team_name: teamName || undefined,
      invite_token: inviteToken || undefined,
    }),
  });
}

export async function getMe(): Promise<AuthUser> {
  return request("/api/auth/me");
}

// --- Team API functions ---

export async function getTeamMembers(): Promise<TeamMember[]> {
  return request("/api/team/members");
}

export async function inviteTeamMember(
  email: string
): Promise<InviteResponse> {
  return request("/api/team/invite", {
    method: "POST",
    body: JSON.stringify({ email }),
  });
}

export async function removeTeamMember(userId: string): Promise<void> {
  return request(`/api/team/members/${userId}`, { method: "DELETE" });
}

// --- Types ---

export interface AuditProgress {
  current: number;
  total: number;
  current_url: string;
}

export interface AuditResult {
  url: string;
  page_intent: string;
  visibility_diagnosis: string;
  fix_plan: string;
  json_ld: unknown;
  json_ld_corrections: { transform: string; detail: string }[];
  flattened_schema: string;
  best_practices: string;
  suggested_concepts: string[];
  suggested_qids: { concept: string; candidates: WikidataEntity[] }[];
  used_qids: { name: string; qid: string }[];
  rendered_html_available: boolean;
  raw_response: string;
}

export interface JobResponse {
  job_id: string;
  project_id?: string | null;
  status: "pending" | "running" | "completed" | "failed";
  progress: AuditProgress;
  results: AuditResult[];
  errors: { url: string; stage: string; message: string }[];
  created_at?: string;
  completed_at?: string;
  user_name?: string | null;
}

export interface SeedCrawlResponse {
  mode: "seed_crawl";
  seed_url: string;
  candidate_urls: string[];
}

export interface WikidataEntity {
  qid: string;
  label: string;
  description: string;
}

export interface WikidataSearchResponse {
  query: string;
  results: WikidataEntity[];
}

export interface ValidateResponse {
  json_ld: unknown;
  corrections: { transform: string; detail: string }[];
  flattened_schema: string;
}

// --- Project types ---

export interface Project {
  id: string;
  name: string;
  description: string;
  created_at: string;
  updated_at: string;
  url_count?: number;
  last_audit?: string | null;
}

export interface ProjectUrl {
  id: string;
  url: string;
  created_at: string;
}

export interface JobSummary {
  id: string;
  status: string;
  url_count?: number;
  progress_total?: number;
  result_count?: number;
  error_count?: number;
  created_at: string;
  completed_at?: string | null;
  user_name?: string | null;
}

export interface ProjectDetail extends Project {
  urls: ProjectUrl[];
  recent_jobs: JobSummary[];
}

export interface UrlHistoryEntry {
  job_id: string;
  job_created_at: string;
  created_at: string;
  data: AuditResult;
}

export interface UrlHistoryResponse {
  url_id: string;
  url: string;
  entries: UrlHistoryEntry[];
}

// --- API functions ---

export async function startAudit(
  urls: string[],
  pathRules?: Record<string, number>,
  projectId?: string
): Promise<{ job_id: string } | SeedCrawlResponse> {
  return request("/api/audit", {
    method: "POST",
    body: JSON.stringify({ urls, path_rules: pathRules, project_id: projectId }),
  });
}

export async function startSeedCrawl(
  seedUrl: string,
  pathRules?: Record<string, number>
): Promise<SeedCrawlResponse> {
  return request("/api/audit", {
    method: "POST",
    body: JSON.stringify({ seed_url: seedUrl, path_rules: pathRules }),
  });
}

export async function getAuditStatus(jobId: string): Promise<JobResponse> {
  return request(`/api/audit/${jobId}`);
}

export async function regenerateReport(
  jobId: string,
  url: string,
  approvedQids: { name: string; qid: string }[],
  projectId?: string
): Promise<AuditResult> {
  return request("/api/audit/report", {
    method: "POST",
    body: JSON.stringify({
      job_id: jobId,
      url,
      approved_qids: approvedQids,
      project_id: projectId,
    }),
  });
}

export async function searchWikidata(
  query: string,
  limit = 5
): Promise<WikidataSearchResponse> {
  return request(`/api/wikidata/search?q=${encodeURIComponent(query)}&limit=${limit}`);
}

export async function validateSchema(
  jsonld: unknown
): Promise<ValidateResponse> {
  return request("/api/schema/validate", {
    method: "POST",
    body: JSON.stringify({ jsonld }),
  });
}

// --- Project API functions ---

export async function createProject(
  name: string,
  description = ""
): Promise<Project> {
  return request("/api/projects", {
    method: "POST",
    body: JSON.stringify({ name, description }),
  });
}

export async function listProjects(): Promise<Project[]> {
  return request("/api/projects");
}

export async function getProject(projectId: string): Promise<ProjectDetail> {
  return request(`/api/projects/${projectId}`);
}

export async function updateProject(
  projectId: string,
  data: { name?: string; description?: string }
): Promise<Project> {
  return request(`/api/projects/${projectId}`, {
    method: "PUT",
    body: JSON.stringify(data),
  });
}

export async function deleteProject(projectId: string): Promise<void> {
  return request(`/api/projects/${projectId}`, { method: "DELETE" });
}

export async function addProjectUrls(
  projectId: string,
  urls: string[]
): Promise<{ added: ProjectUrl[] }> {
  return request(`/api/projects/${projectId}/urls`, {
    method: "POST",
    body: JSON.stringify({ urls }),
  });
}

export async function removeProjectUrl(
  projectId: string,
  urlId: string
): Promise<void> {
  return request(`/api/projects/${projectId}/urls/${urlId}`, {
    method: "DELETE",
  });
}

export async function getUrlHistory(
  projectId: string,
  urlId: string
): Promise<UrlHistoryResponse> {
  return request(`/api/projects/${projectId}/urls/${urlId}/history`);
}

export async function getProjectQids(
  projectId: string,
  url: string
): Promise<{ name: string; qid: string }[]> {
  return request(`/api/projects/${projectId}/qids?url=${encodeURIComponent(url)}`);
}

export async function setProjectQids(
  projectId: string,
  url: string,
  qids: { name: string; qid: string }[]
): Promise<void> {
  return request(`/api/projects/${projectId}/qids`, {
    method: "PUT",
    body: JSON.stringify({ url, qids }),
  });
}
