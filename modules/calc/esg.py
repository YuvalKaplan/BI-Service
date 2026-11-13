from __future__ import annotations
from modules import config


def qualify(disclosure: dict, rating: dict, rules: type = config.Esg) -> tuple[bool, dict]:
    """Whether a company qualifies as ESG under `rules` (PASSING_GRADES, ESG_SCORE_MIN,
    GOVERNANCE_SCORE_MIN): the live pipeline's config.Esg, or BT's own (modules/bt/config.py)."""
    esg_risk_rating = rating.get("ESGRiskRating")       # letter string e.g. "A", "BBB"
    esg_score       = disclosure.get("ESGScore")         # float, higher is better
    governance_score = disclosure.get("governanceScore") # float, higher is better

    factors = {
        "esg_risk_rating":  esg_risk_rating,
        "esg_score":        esg_score,
        "governance_score": governance_score,
    }

    if all(v is None for v in factors.values()):
        return False, factors

    checks = [
        esg_risk_rating  is None or esg_risk_rating in rules.PASSING_GRADES,
        esg_score        is None or esg_score        >= rules.ESG_SCORE_MIN,
        governance_score is None or governance_score >= rules.GOVERNANCE_SCORE_MIN,
    ]
    return all(checks), factors
