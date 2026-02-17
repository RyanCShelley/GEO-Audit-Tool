import { useState } from "react";
import type { WikidataEntity } from "../api/client";
import { searchWikidata } from "../api/client";

interface SuggestedGroup {
  concept: string;
  candidates: WikidataEntity[];
}

interface ApprovedQid {
  name: string;
  qid: string;
}

interface Props {
  suggestedQids: SuggestedGroup[];
  onApprove: (approved: ApprovedQid[]) => void;
  loading?: boolean;
}

export default function QidReview({ suggestedQids, onApprove, loading }: Props) {
  const [selected, setSelected] = useState<Map<string, ApprovedQid>>(() => {
    // Pre-select the first candidate for each concept
    const map = new Map<string, ApprovedQid>();
    for (const group of suggestedQids) {
      if (group.candidates.length > 0) {
        const c = group.candidates[0];
        map.set(c.qid, { name: group.concept, qid: c.qid });
      }
    }
    return map;
  });

  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<WikidataEntity[]>([]);
  const [searching, setSearching] = useState(false);

  function toggleQid(concept: string, qid: string) {
    setSelected((prev) => {
      const next = new Map(prev);
      if (next.has(qid)) {
        next.delete(qid);
      } else {
        next.set(qid, { name: concept, qid });
      }
      return next;
    });
  }

  async function handleSearch() {
    if (!searchQuery.trim()) return;
    setSearching(true);
    try {
      const res = await searchWikidata(searchQuery.trim());
      setSearchResults(res.results);
    } catch {
      setSearchResults([]);
    } finally {
      setSearching(false);
    }
  }

  function handleApprove() {
    onApprove(Array.from(selected.values()));
  }

  return (
    <div className="qid-review">
      <h3>Review Wikidata QIDs</h3>
      <p className="qid-review__description">
        Select which Wikidata entities to include in the JSON-LD "about" array.
        These link your content to the knowledge graph for GEO visibility.
      </p>

      {suggestedQids.map((group) => (
        <div key={group.concept} className="qid-group">
          <h4>{group.concept}</h4>
          {group.candidates.length === 0 && (
            <p className="qid-group__empty">No Wikidata matches found.</p>
          )}
          {group.candidates.map((c) => (
            <label key={c.qid} className="qid-option">
              <input
                type="checkbox"
                checked={selected.has(c.qid)}
                onChange={() => toggleQid(group.concept, c.qid)}
              />
              <span className="qid-option__label">
                <strong>{c.label}</strong> ({c.qid})
                {c.description && <span className="qid-option__desc"> — {c.description}</span>}
              </span>
            </label>
          ))}
        </div>
      ))}

      <div className="qid-search">
        <h4>Search Wikidata for more</h4>
        <div className="qid-search__row">
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSearch()}
            placeholder="e.g. Payment Processing"
          />
          <button onClick={handleSearch} disabled={searching}>
            {searching ? "Searching..." : "Search"}
          </button>
        </div>
        {searchResults.map((r) => (
          <label key={r.qid} className="qid-option">
            <input
              type="checkbox"
              checked={selected.has(r.qid)}
              onChange={() => toggleQid(r.label, r.qid)}
            />
            <span className="qid-option__label">
              <strong>{r.label}</strong> ({r.qid})
              {r.description && <span className="qid-option__desc"> — {r.description}</span>}
            </span>
          </label>
        ))}
      </div>

      <button
        className="btn btn--primary"
        onClick={handleApprove}
        disabled={selected.size === 0 || loading}
      >
        {loading ? "Generating..." : `Generate Final Schema (${selected.size} QIDs)`}
      </button>
    </div>
  );
}
