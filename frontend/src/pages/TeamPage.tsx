import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";
import {
  getTeamMembers,
  inviteTeamMember,
  removeTeamMember,
  type TeamMember,
} from "../api/client";

export default function TeamPage() {
  const { user } = useAuth();
  const navigate = useNavigate();
  const [members, setMembers] = useState<TeamMember[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // Invite state
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteLink, setInviteLink] = useState("");
  const [inviting, setInviting] = useState(false);

  useEffect(() => {
    if (user?.role !== "admin") {
      navigate("/");
      return;
    }
    loadMembers();
  }, [user, navigate]);

  async function loadMembers() {
    try {
      const data = await getTeamMembers();
      setMembers(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load members");
    } finally {
      setLoading(false);
    }
  }

  async function handleInvite(e: React.FormEvent) {
    e.preventDefault();
    if (!inviteEmail.trim()) return;
    setInviting(true);
    setError("");
    setInviteLink("");
    try {
      const res = await inviteTeamMember(inviteEmail.trim());
      const origin = window.location.origin;
      setInviteLink(`${origin}/login?invite=${res.invite_token}`);
      setInviteEmail("");
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create invite");
    } finally {
      setInviting(false);
    }
  }

  async function handleRemove(memberId: string, memberName: string) {
    if (!confirm(`Remove ${memberName} from the team?`)) return;
    try {
      await removeTeamMember(memberId);
      setMembers((prev) => prev.filter((m) => m.id !== memberId));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to remove member");
    }
  }

  function copyInviteLink() {
    navigator.clipboard.writeText(inviteLink);
  }

  if (loading) return <div className="team-page"><p>Loading...</p></div>;

  return (
    <div className="team-page">
      <h1>Team Management</h1>

      {error && <p className="error">{error}</p>}

      {/* Invite section */}
      <section className="team-section">
        <h2>Invite Member</h2>
        <form onSubmit={handleInvite} className="team-invite-form">
          <input
            type="email"
            value={inviteEmail}
            onChange={(e) => setInviteEmail(e.target.value)}
            placeholder="Email address"
            required
          />
          <button
            type="submit"
            className="btn btn--primary"
            disabled={inviting || !inviteEmail.trim()}
          >
            {inviting ? "Creating..." : "Generate Invite Link"}
          </button>
        </form>
        {inviteLink && (
          <div className="team-invite-result">
            <p>Share this link with the invitee:</p>
            <div className="team-invite-link">
              <code>{inviteLink}</code>
              <button className="btn btn--small" onClick={copyInviteLink}>
                Copy
              </button>
            </div>
          </div>
        )}
      </section>

      {/* Members table */}
      <section className="team-section">
        <h2>Members ({members.length})</h2>
        <table className="history-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Email</th>
              <th>Role</th>
              <th>Joined</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {members.map((m) => (
              <tr key={m.id}>
                <td>{m.name}</td>
                <td>{m.email}</td>
                <td>
                  <span className={`status-badge status-badge--${m.role === "admin" ? "running" : "completed"}`}>
                    {m.role}
                  </span>
                </td>
                <td>{new Date(m.created_at).toLocaleDateString()}</td>
                <td>
                  {m.id !== user?.id && (
                    <button
                      className="btn btn--small btn--danger"
                      onClick={() => handleRemove(m.id, m.name)}
                    >
                      Remove
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}
