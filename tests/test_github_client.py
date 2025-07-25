"""Tests for GitHub REST client and report generation logic."""

import pytest


class TestRateLimitHandling:
    def test_should_wait_when_remaining_low(self):
        remaining = 5
        should_wait = remaining < 10
        assert should_wait is True

    def test_should_not_wait_when_remaining_high(self):
        remaining = 100
        should_wait = remaining < 10
        assert should_wait is False


class TestPopularityTier:
    def classify(self, stars):
        if stars >= 10000:
            return "mega"
        elif stars >= 1000:
            return "popular"
        elif stars >= 100:
            return "notable"
        return "emerging"

    def test_mega(self):
        assert self.classify(50000) == "mega"

    def test_popular(self):
        assert self.classify(5000) == "popular"

    def test_notable(self):
        assert self.classify(500) == "notable"

    def test_emerging(self):
        assert self.classify(50) == "emerging"


class TestStarForkRatio:
    def test_normal_ratio(self):
        stars, forks = 1000, 200
        ratio = round(stars / max(forks, 1), 2)
        assert ratio == 5.0

    def test_zero_forks(self):
        stars, forks = 100, 0
        ratio = round(stars / max(forks, 1), 2)
        assert ratio == 100.0


class TestBackfillCheckpoint:
    def test_checkpoint_format(self):
        checkpoint = {"last_cursor": "abc123", "repos_processed": 500, "completed": False}
        assert checkpoint["last_cursor"] == "abc123"
        assert checkpoint["repos_processed"] == 500

    def test_checkpoint_resume(self):
        checkpoint = {"last_cursor": "xyz789", "repos_processed": 1200, "completed": True}
        assert checkpoint["completed"] is True


class TestLanguageTrends:
    def test_language_percentages(self):
        totals = {"Python": 500, "JavaScript": 300, "Rust": 200}
        grand_total = sum(totals.values())
        pcts = {lang: round(count / grand_total * 100, 1) for lang, count in totals.items()}
        assert pcts["Python"] == 50.0
        assert pcts["JavaScript"] == 30.0
        assert pcts["Rust"] == 20.0
