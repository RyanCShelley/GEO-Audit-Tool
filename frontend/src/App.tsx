import { BrowserRouter, Routes, Route, Link, Navigate } from "react-router-dom";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import LoginPage from "./pages/LoginPage";
import ProjectListPage from "./pages/ProjectListPage";
import ProjectDetailPage from "./pages/ProjectDetailPage";
import UrlHistoryPage from "./pages/UrlHistoryPage";
import AuditPage from "./pages/AuditPage";
import ReportPage from "./pages/ReportPage";
import TeamPage from "./pages/TeamPage";
import "./App.css";

function AppRoutes() {
  const { user, loading, logout } = useAuth();

  if (loading) {
    return <div className="app-loading">Loading...</div>;
  }

  if (!user) {
    return (
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="*" element={<Navigate to="/login" replace />} />
      </Routes>
    );
  }

  return (
    <>
      <header className="app-header">
        <Link to="/" className="app-header__logo">
          GEO Audit Tool
        </Link>
        <nav className="app-header__nav">
          <Link to="/">Projects</Link>
          <Link to="/audit">Quick Audit</Link>
          {user.role === "admin" && <Link to="/team">Team</Link>}
          <button onClick={logout} className="app-header__logout">
            Logout
          </button>
        </nav>
      </header>
      <main className="app-main">
        <Routes>
          <Route path="/" element={<ProjectListPage />} />
          <Route path="/audit" element={<AuditPage />} />
          <Route path="/report/:jobId" element={<ReportPage />} />
          <Route path="/project/:projectId" element={<ProjectDetailPage />} />
          <Route path="/project/:projectId/audit" element={<AuditPage />} />
          <Route path="/project/:projectId/report/:jobId" element={<ReportPage />} />
          <Route path="/project/:projectId/url/:urlId/history" element={<UrlHistoryPage />} />
          <Route path="/team" element={<TeamPage />} />
          <Route path="/login" element={<Navigate to="/" replace />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </>
  );
}

function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <AppRoutes />
      </AuthProvider>
    </BrowserRouter>
  );
}

export default App;
