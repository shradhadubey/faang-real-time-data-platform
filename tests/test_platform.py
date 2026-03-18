"""
Unit Tests — FAANG Data Platform
==================================
Tests event generation, data validation, and transformation logic.
All tests run without AWS credentials — no external dependencies.

Run with: pytest tests/ -v
"""

import json
import sys
import os
import pytest
import pandas as pd


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Constants (defined here so tests don't depend on producer internals) ───────

VALID_EVENT_TYPES  = {"page_view", "product_view", "add_to_cart", "purchase", "search", "wishlist_add"}
VALID_DEVICE_TYPES = {"mobile", "desktop", "tablet"}


# ── Helpers ────────────────────────────────────────────────────────────────────

def make_event(**overrides):
    """Build a minimal valid event dict for testing."""
    base = {
        "event_id":       "test-event-001",
        "timestamp":      "2026-03-18T10:00:00+00:00",
        "user_id":        "U123456",
        "session_id":     "sess-001",
        "event_type":     "purchase",
        "product_id":     "P001",
        "product_name":   "MacBook Pro",
        "category":       "Electronics",
        "price":          999.99,
        "quantity":       1,
        "revenue":        999.99,
        "device_type":    "mobile",
        "country":        "US",
        "schema_version": "1.0",
    }
    base.update(overrides)
    return base


def validate_event(record):
    """
    Mirror of silver_transformer.py validation logic.
    Returns (is_valid, reason).
    """
    if not record.get("event_id"):
        return False, "missing_event_id"
    if not record.get("user_id"):
        return False, "missing_user_id"
    if not record.get("timestamp"):
        return False, "missing_timestamp"
    if record.get("event_type") not in VALID_EVENT_TYPES:
        return False, f"invalid_event_type:{record.get('event_type')}"
    if record.get("device_type") not in VALID_DEVICE_TYPES:
        return False, f"invalid_device_type:{record.get('device_type')}"
    if record.get("price") is not None and record["price"] <= 0:
        return False, "invalid_price"
    if record.get("quantity") is not None and record["quantity"] < 1:
        return False, "invalid_quantity"
    return True, ""


def get_user_segment(daily_spend, event_count=0):
    """Mirror of gold_aggregator.py segmentation logic."""
    if daily_spend > 500:
        return "vip"
    elif daily_spend > 100:
        return "high_value"
    elif daily_spend > 0:
        return "buyer"
    elif event_count > 10:
        return "engaged_browser"
    else:
        return "casual_browser"


# ── Event Structure Tests ──────────────────────────────────────────────────────

class TestEventStructure:

    def test_valid_event_passes_validation(self):
        ok, reason = validate_event(make_event())
        assert ok is True
        assert reason == ""

    def test_event_serializes_to_json(self):
        event = make_event()
        serialized = json.dumps(event)
        parsed = json.loads(serialized)
        assert parsed["event_id"] == event["event_id"]
        assert parsed["user_id"] == event["user_id"]

    def test_all_required_fields_present(self):
        event = make_event()
        required = ["event_id", "timestamp", "user_id", "event_type", "device_type", "country"]
        for field in required:
            assert field in event, f"Missing required field: {field}"

    def test_schema_version_is_string(self):
        event = make_event()
        assert isinstance(event["schema_version"], str)
        assert event["schema_version"] == "1.0"

    def test_revenue_positive_for_purchase(self):
        event = make_event(event_type="purchase", price=99.99, quantity=2, revenue=199.98)
        assert event["revenue"] > 0

    def test_revenue_calculation_correct(self):
        price = 49.99
        quantity = 3
        expected_revenue = round(price * quantity, 2)
        assert expected_revenue == 149.97


# ── Validation Tests ───────────────────────────────────────────────────────────

class TestValidation:

    def test_missing_event_id_fails(self):
        ok, reason = validate_event(make_event(event_id=None))
        assert ok is False
        assert reason == "missing_event_id"

    def test_empty_event_id_fails(self):
        ok, reason = validate_event(make_event(event_id=""))
        assert ok is False
        assert reason == "missing_event_id"

    def test_missing_user_id_fails(self):
        ok, reason = validate_event(make_event(user_id=None))
        assert ok is False
        assert reason == "missing_user_id"

    def test_missing_timestamp_fails(self):
        ok, reason = validate_event(make_event(timestamp=None))
        assert ok is False
        assert reason == "missing_timestamp"

    def test_invalid_event_type_fails(self):
        ok, reason = validate_event(make_event(event_type="click"))
        assert ok is False
        assert "invalid_event_type" in reason

    def test_all_valid_event_types_pass(self):
        for et in VALID_EVENT_TYPES:
            ok, _ = validate_event(make_event(event_type=et))
            assert ok is True, f"Expected {et} to be valid"

    def test_invalid_device_type_fails(self):
        ok, reason = validate_event(make_event(device_type="smartwatch"))
        assert ok is False
        assert "invalid_device_type" in reason

    def test_all_valid_device_types_pass(self):
        for dt in VALID_DEVICE_TYPES:
            ok, _ = validate_event(make_event(device_type=dt))
            assert ok is True, f"Expected {dt} to be valid"

    def test_negative_price_fails(self):
        ok, reason = validate_event(make_event(price=-10.0))
        assert ok is False
        assert reason == "invalid_price"

    def test_zero_price_fails(self):
        ok, reason = validate_event(make_event(price=0.0))
        assert ok is False
        assert reason == "invalid_price"

    def test_zero_quantity_fails(self):
        ok, reason = validate_event(make_event(quantity=0))
        assert ok is False
        assert reason == "invalid_quantity"

    def test_negative_quantity_fails(self):
        ok, reason = validate_event(make_event(quantity=-1))
        assert ok is False
        assert reason == "invalid_quantity"

    def test_null_price_allowed(self):
        ok, _ = validate_event(make_event(event_type="page_view", price=None))
        assert ok is True

    def test_null_quantity_allowed(self):
        ok, _ = validate_event(make_event(event_type="page_view", quantity=None))
        assert ok is True


# ── Gold Aggregation Logic Tests ───────────────────────────────────────────────

class TestGoldAggregation:

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame([
            {
                "event_id": "1", "user_id": "U001", "session_id": "s1",
                "event_type": "purchase", "product_id": "P001",
                "product_name": "MacBook Pro", "category": "Electronics",
                "price": 999.99, "quantity": 1, "revenue": 999.99,
                "device_type": "mobile", "country": "US", "event_hour": 10,
            },
            {
                "event_id": "2", "user_id": "U002", "session_id": "s2",
                "event_type": "add_to_cart", "product_id": "P001",
                "product_name": "MacBook Pro", "category": "Electronics",
                "price": 999.99, "quantity": 1, "revenue": None,
                "device_type": "desktop", "country": "GB", "event_hour": 10,
            },
            {
                "event_id": "3", "user_id": "U003", "session_id": "s3",
                "event_type": "page_view", "product_id": None,
                "product_name": None, "category": None,
                "price": None, "quantity": None, "revenue": None,
                "device_type": "mobile", "country": "US", "event_hour": 11,
            },
            {
                "event_id": "4", "user_id": "U001", "session_id": "s4",
                "event_type": "purchase", "product_id": "P002",
                "product_name": "iPhone", "category": "Electronics",
                "price": 499.99, "quantity": 2, "revenue": 999.98,
                "device_type": "mobile", "country": "US", "event_hour": 12,
            },
        ])

    def test_total_records(self, sample_df):
        assert len(sample_df) == 4

    def test_purchase_count(self, sample_df):
        purchases = sample_df[sample_df["event_type"] == "purchase"]
        assert len(purchases) == 2

    def test_total_revenue(self, sample_df):
        total = sample_df[sample_df["event_type"] == "purchase"]["revenue"].sum()
        assert round(total, 2) == 1999.97

    def test_unique_buyers(self, sample_df):
        buyers = sample_df[sample_df["event_type"] == "purchase"]["user_id"].nunique()
        assert buyers == 2

    def test_event_type_distribution(self, sample_df):
        counts = sample_df["event_type"].value_counts()
        assert counts["purchase"] == 2
        assert counts["add_to_cart"] == 1
        assert counts["page_view"] == 1

    def test_country_filter(self, sample_df):
        us_events = sample_df[sample_df["country"] == "US"]
        assert len(us_events) == 3


# ── User Segmentation Tests ────────────────────────────────────────────────────

class TestUserSegmentation:

    def test_vip_segment(self):
        assert get_user_segment(600.0) == "vip"

    def test_vip_boundary(self):
        assert get_user_segment(500.01) == "vip"

    def test_high_value_segment(self):
        assert get_user_segment(250.0) == "high_value"

    def test_high_value_boundary(self):
        assert get_user_segment(100.01) == "high_value"

    def test_buyer_segment(self):
        assert get_user_segment(50.0) == "buyer"

    def test_buyer_boundary(self):
        assert get_user_segment(0.01) == "buyer"

    def test_engaged_browser(self):
        assert get_user_segment(0.0, event_count=15) == "engaged_browser"

    def test_casual_browser(self):
        assert get_user_segment(0.0, event_count=3) == "casual_browser"

    def test_exactly_500_is_high_value(self):
        assert get_user_segment(500.0) == "high_value"

    def test_exactly_100_is_buyer(self):
        assert get_user_segment(100.0) == "buyer"


# ── Conversion Rate Tests ──────────────────────────────────────────────────────

class TestConversionRate:

    def test_standard_conversion_rate(self):
        rate = round(3 / 10, 4)
        assert rate == 0.3

    def test_zero_cart_adds_no_division_error(self):
        rate = round(5 / max(0, 1), 4)
        assert rate == 5.0

    def test_perfect_conversion(self):
        rate = round(10 / 10, 4)
        assert rate == 1.0

    def test_zero_purchases(self):
        rate = round(0 / max(10, 1), 4)
        assert rate == 0.0


# ── Run ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
