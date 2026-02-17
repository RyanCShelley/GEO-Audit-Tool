import { BrowserRouter, Routes, Route, Link } from "react-router-dom";
import ProjectListPage from "./pages/ProjectListPage";
import ProjectDetailPage from "./pages/ProjectDetailPage";
import UrlHistoryPage from "./pages/UrlHistoryPage";
import AuditPage from "./pages/AuditPage";
import ReportPage from "./pages/ReportPage";
import "./App.css";

function App() {
  return (
    <BrowserRouter>
      <header className="app-header">
        <Link to="/" className="app-header__logo">
          GEO Audit Tool
        </Link>
        <nav className="app-header__nav">
          <Link to="/">Projects</Link>
          <Link to="/audit">Quick Audit</Link>
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
        </Routes>
      </main>
    </BrowserRouter>
  );
}

export default App;
