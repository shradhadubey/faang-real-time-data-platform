"""
Unit Tests — FAANG Data Platform
==================================
Tests for event generation, data quality logic, and batch aggregations.
Run with: pytest tests/ -v
"""

import json
import pytest
from datetime import date
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data_generator.streaming_producer import (
    generate_event,
    ECommerceEvent,
    weighted_choice,
    VALID_EVENT_TYPES := ["page_view", "product_view", "add_to_cart", "purchase", "search", "wishlist_add"],
)


# ── Event Generator Tests ──────────────────────────────────────────────────────

class TestEventGeneration:
    def test_generate_event_returns_ecommerce_event(self):
        event = generate_event()
        assert isinstance(event, ECommerceEvent)

    def test_event_has_required_fields(self):
        event = generate_event()
        assert event.event_id is not None
        assert event.timestamp is not None
        assert event.user_id is not None
        assert event.event_type is not None
        assert event.device_type is not None
        assert event.country is not None

    def test_event_id_is_uuid(self):
        import uuid
        event = generate_event()
        # Should not raise ValueError
        uuid.UUID(event.event_id)

    def test_event_type_is_valid(self):
        valid_types = {"page_view", "product_view", "add_to_cart", "purchase", "search", "wishlist_add"}
        for _ in range(100):
            event = generate_event()
            assert event.event_type in valid_types

    def test_device_type_is_valid(self):
        valid_devices = {"mobile", "desktop", "tablet"}
        for _ in range(100):
            event = generate_event()
            assert event.device_type in valid_devices

    def test_purchase_events_have_revenue(self):
        """Purchase events must always have a revenue value."""
        purchase_events = []
        attempts = 0
        while len(purchase_events) < 5 and attempts < 1000:
            event = generate_event()
            if event.event_type == "purchase":
                purchase_events.append(event)
            attempts += 1

        for event in purchase_events:
            assert event.revenue is not None, "Purchase event missing revenue"
            assert event.revenue > 0, "Purchase revenue must be positive"
            assert event.price is not None
            assert event.quantity is not None

    def test_page_view_has_no_product(self):
        """Page view events should not have product data."""
        # Generate many events and check page_views
        page_views = []
        attempts = 0
        while len(page_views) < 5 and attempts < 500:
            event = generate_event()
            if event.event_type == "page_view":
                page_views.append(event)
            attempts += 1

        # page_view should not have product_id
        for event in page_views:
            assert event.product_id is None

    def test_price_is_positive_when_present(self):
        for _ in range(200):
            event = generate_event()
            if event.price is not None:
                assert event.price > 0

    def test_quantity_is_positive_when_present(self):
        for _ in range(200):
            event = generate_event()
            if event.quantity is not None:
                assert event.quantity >= 1

    def test_country_is_two_chars(self):
        for _ in range(100):
            event = generate_event()
            assert len(event.country) == 2

    def test_event_serializes_to_valid_json(self):
        event = generate_event()
        json_str = event.to_json()
        parsed = json.loads(json_str)
        assert parsed["event_id"] == event.event_id
        assert parsed["user_id"] == event.user_id

    def test_schema_version_present(self):
        event = generate_event()
        assert event.schema_version == "1.0"

    def test_generate_1000_events_quickly(self):
        import time
        start = time.time()
        events = [generate_event() for _ in range(1000)]
        elapsed = time.time() - start
        assert len(events) == 1000
        assert elapsed < 2.0, f"1000 events took {elapsed:.2f}s — too slow"


# ── Weighted Choice Tests ──────────────────────────────────────────────────────

class TestWeightedChoice:
    def test_returns_valid_value(self):
        choices = [("a", 0.5), ("b", 0.3), ("c", 0.2)]
        result = weighted_choice(choices)
        assert result in ("a", "b", "c")

    def test_distribution_roughly_correct(self):
        """Heavy item should appear most often."""
        choices = [("heavy", 0.9), ("light", 0.1)]
        results = [weighted_choice(choices) for _ in range(1000)]
        heavy_count = results.count("heavy")
        # Should be roughly 90% — accept wide range
        assert 750 < heavy_count < 1000


# ── Kinesis Producer Tests ──────────────────────────────────────────────────────

class TestKinesisProducer:
    @patch("data_generator.streaming_producer.boto3.client")
    def test_put_records_success(self, mock_boto):
        from data_generator.streaming_producer import KinesisProducer
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        mock_client.put_records.return_value = {"FailedRecordCount": 0, "Records": []}

        producer = KinesisProducer("test-stream", "us-east-1")
        events = [generate_event() for _ in range(5)]
        producer.put_records_batch(events)

        assert producer.total_sent == 5
        assert producer.total_failed == 0

    @patch("data_generator.streaming_producer.boto3.client")
    def test_partial_failure_retried(self, mock_boto):
        from data_generator.streaming_producer import KinesisProducer
        mock_client = MagicMock()
        mock_boto.return_value = mock_client

        # First call: 1 failure; second call: success
        mock_client.put_records.side_effect = [
            {
                "FailedRecordCount": 1,
                "Records": [
                    {"SequenceNumber": "1"},
                    {"ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "throttled"},
                ],
            },
            {"FailedRecordCount": 0, "Records": []},
        ]

        producer = KinesisProducer("test-stream", "us-east-1")
        events = [generate_event() for _ in range(2)]

        with patch("time.sleep"):   # Don't actually sleep in tests
            producer.put_records_batch(events)

        assert mock_client.put_records.call_count == 2


# ── Run tests ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
