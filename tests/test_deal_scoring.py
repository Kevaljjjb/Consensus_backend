from api import deal_scoring


_SAMPLE_LISTING = {
    "title": "HVAC Services Business",
    "industry": "Home Services",
    "city": "Dallas",
    "state": "TX",
    "country": "US",
    "source": "BizBuySell",
    "price": "$6,000,000",
    "gross_revenue": "$8,000,000",
    "cash_flow": "$2,500,000",
    "inventory": "$150,000",
    "ebitda": "$2,200,000",
    "description": "Established HVAC service business with recurring maintenance contracts and repeat customers.",
    "financial_data": "Revenue stable for the last 3 years.",
    "extra_information": "Founded in 2008. Mission-critical service work.",
    "deal_date": "2026-03-01",
    "first_seen_date": "2026-03-02T00:00:00+00:00",
    "source_link": "https://example.com/listing/1",
    "cash_flow_num": 2500000,
    "gross_revenue_num": 8000000,
    "price_num": 6000000,
}


class _FakeMessage:
    def __init__(self, content):
        self.content = content


class _FakeChoice:
    def __init__(self, content):
        self.message = _FakeMessage(content)


class _FakeResponse:
    def __init__(self, content):
        self.choices = [_FakeChoice(content)]


class _FakeCompletions:
    def __init__(self, content, recorder):
        self._content = content
        self._recorder = recorder

    def create(self, **kwargs):
        self._recorder.update(kwargs)
        return _FakeResponse(self._content)


class _FakeChat:
    def __init__(self, content, recorder):
        self.completions = _FakeCompletions(content, recorder)


class _FakeClient:
    def __init__(self, content, recorder):
        self.chat = _FakeChat(content, recorder)


def test_evaluate_deal_parses_valid_model_json(monkeypatch):
    recorded: dict = {}
    content = """
    {
      "fit_score": 78,
      "score_breakdown": {
        "cash_flow": 20,
        "profitability": 18,
        "maturity": 13,
        "locality": 15,
        "stability": 12
      },
      "pros": [
        "Reported cash flow of $2.5M sits inside Tucker's preferred range.",
        "Dallas, TX keeps the opportunity inside Tucker's target US geography.",
        "Recurring HVAC maintenance contracts support revenue stability."
      ],
      "cons": [
        "Gross revenue of $8.0M implies only moderate scale for a platform investment.",
        "Customer concentration is not disclosed in the listing.",
        "Supplier diversification is not described in the deal materials."
      ],
      "summary": "This is worth pursuing because the deal fits Tucker's cash flow and geography screens, but diligence should focus on concentration risk."
    }
    """
    monkeypatch.setattr(deal_scoring, "_get_client", lambda: _FakeClient(content, recorded))
    monkeypatch.setattr(deal_scoring, "_get_model", lambda: "Qwen/Qwen3.5-27B")

    result = deal_scoring.evaluate_deal(_SAMPLE_LISTING)

    assert result["fit_score"] == 78
    assert result["score_breakdown"]["cash_flow"] == 20
    assert result["model_used"] == "Qwen/Qwen3.5-27B"
    assert result["pros"][0].startswith("Reported cash flow")
    assert recorded["model"] == "Qwen/Qwen3.5-27B"
    assert recorded["messages"][0]["role"] == "system"
    assert "HVAC Services Business" in recorded["messages"][1]["content"]


def test_evaluate_deal_falls_back_when_model_payload_is_invalid(monkeypatch):
    recorded: dict = {}
    invalid_content = """
    {
      "fit_score": 105,
      "score_breakdown": {
        "cash_flow": 40,
        "profitability": 18,
        "maturity": 13,
        "locality": 15,
        "stability": 12
      },
      "pros": ["Only one pro"],
      "cons": ["Only one con"],
      "summary": ""
    }
    """
    monkeypatch.setattr(deal_scoring, "_get_client", lambda: _FakeClient(invalid_content, recorded))
    monkeypatch.setattr(deal_scoring, "_get_model", lambda: "Qwen/Qwen3.5-27B")

    result = deal_scoring.evaluate_deal(_SAMPLE_LISTING)

    assert result["model_used"] == "heuristic-fallback"
    assert len(result["pros"]) >= 3
    assert len(result["cons"]) >= 3
    assert sum(result["score_breakdown"].values()) == result["fit_score"]
    assert "Dallas, TX, US" in result["summary"]
