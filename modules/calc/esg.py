from __future__ import annotations

ESG_PASSING_GRADES              = {"AAA", "AA", "A", "BBB"}
ESG_INDUSTRY_RANK_PERCENTILE_MAX = 0.40   # top 40% of industry (rank/total <= 0.40)
ESG_SCORE_MIN                   = 50.0
GOVERNANCE_SCORE_MIN            = 50.0


def _parse_industry_rank(value: str | None) -> float | None:
    """Parse "X out of Y" string → X/Y ratio. Returns None if unparseable."""
    if not value:
        return None
    try:
        parts = value.lower().replace("out of", "/").split("/")
        return float(parts[0].strip()) / float(parts[1].strip())
    except Exception:
        return None


def qualify(disclosure: dict, rating: dict) -> tuple[bool, dict]:
    esg_risk_rating   = rating.get("ESGRiskRating")       # letter string e.g. "A", "BBB"
    industry_rank_raw = rating.get("industryRank")         # string e.g. "4 out of 15"
    esg_score         = disclosure.get("ESGScore")         # float, higher is better
    governance_score  = disclosure.get("governanceScore")  # float, higher is better

    factors = {
        "esg_risk_rating":  esg_risk_rating,
        "industry_rank":    industry_rank_raw,
        "esg_score":        esg_score,
        "governance_score": governance_score,
    }

    if all(v is None for v in factors.values()):
        return False, factors

    industry_rank_ratio = _parse_industry_rank(industry_rank_raw)

    checks = [
        esg_risk_rating   is None or esg_risk_rating in ESG_PASSING_GRADES,
        industry_rank_raw is None or (industry_rank_ratio is not None and industry_rank_ratio <= ESG_INDUSTRY_RANK_PERCENTILE_MAX),
        esg_score         is None or esg_score        >= ESG_SCORE_MIN,
        governance_score  is None or governance_score >= GOVERNANCE_SCORE_MIN,
    ]
    return all(checks), factors
