"""
Diagnostics Aggregator
Fan-out module that collects health metrics from multiple sources.

v3.14: Operational Readiness
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from .error_buffer import error_buffer
from .onboarding_metrics import OnboardingMetricsService

logger = logging.getLogger(__name__)


class DiagnosticsAggregator:
    """
    Collects and aggregates diagnostics from all health sources.

    Returns a combined dict with:
    - error_counts: Error buffer statistics
    - onboarding_funnel: Onboarding completion metrics
    - system_health: System-level health indicators
    - recent_errors: Most recent error entries
    """

    def __init__(self, db=None):
        """
        Initialize with optional database connection.

        Args:
            db: Database instance (required for onboarding metrics)
        """
        self.db = db

    def collect_all(self, include_errors: bool = True, error_limit: int = 20) -> dict:
        """
        Collect diagnostics from all sources.

        Args:
            include_errors: Whether to include recent error entries
            error_limit: Maximum number of error entries to return

        Returns:
            Combined diagnostics dict
        """
        result = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "error_counts": self._get_error_counts(),
            "innovator_stats": self._get_innovator_stats(),
            "onboarding_funnel": self._get_onboarding_funnel(),
            "system_health": self._get_system_health(),
        }

        if include_errors:
            result["recent_errors"] = self._get_recent_errors(error_limit)

        return result

    def _get_error_counts(self) -> dict:
        """
        Get error counts from the error buffer.

        Returns:
            Dict with counts by level
        """
        try:
            return error_buffer.get_counts()
        except Exception as e:
            logger.warning(f"Error buffer access failed: {e}")
            return {"ERROR": 0, "WARNING": 0, "CRITICAL": 0, "error": str(e)}

    def _get_innovator_stats(self) -> dict:
        """
        Get innovator (user) statistics.

        Returns:
            Dict with total users and activity breakdowns
        """
        if self.db is None:
            return {
                "status": "unavailable",
                "reason": "Database connection not provided"
            }

        try:
            from datetime import timedelta
            raw_db = self.db.db if hasattr(self.db, 'db') else self.db
            now = datetime.now(timezone.utc)

            # Total registered users
            total_users = raw_db.users.count_documents({})

            # Active in last 24 hours
            active_24h = raw_db.users.count_documents({
                "last_active": {"$gte": now - timedelta(hours=24)}
            })

            # Active in last 7 days
            active_7d = raw_db.users.count_documents({
                "last_active": {"$gte": now - timedelta(days=7)}
            })

            # Active in last 30 days
            active_30d = raw_db.users.count_documents({
                "last_active": {"$gte": now - timedelta(days=30)}
            })

            # New registrations this week
            new_this_week = raw_db.users.count_documents({
                "created_at": {"$gte": now - timedelta(days=7)}
            })

            return {
                "total": total_users,
                "active_24h": active_24h,
                "active_7d": active_7d,
                "active_30d": active_30d,
                "new_this_week": new_this_week,
            }
        except Exception as e:
            logger.warning(f"Innovator stats access failed: {e}")
            return {
                "status": "error",
                "reason": str(e)
            }

    def _get_onboarding_funnel(self) -> dict:
        """
        Get onboarding funnel statistics.

        Returns:
            Funnel stats dict or placeholder if db not available
        """
        if self.db is None:
            return {
                "status": "unavailable",
                "reason": "Database connection not provided"
            }

        try:
            metrics_service = OnboardingMetricsService(self.db)
            return metrics_service.get_funnel_stats_sync(days=30)
        except Exception as e:
            logger.warning(f"Onboarding metrics access failed: {e}")
            return {
                "status": "error",
                "reason": str(e)
            }

    def _get_system_health(self) -> dict:
        """
        Get system-level health indicators.

        Placeholder for future metrics like:
        - Database connection status
        - Redis connection status
        - LLM gateway availability
        - License status

        Returns:
            System health dict
        """
        health = {
            "database": self._check_database_health(),
            "license": self._check_license_status(),
        }

        # Calculate overall status
        statuses = [v.get("status", "unknown") for v in health.values() if isinstance(v, dict)]
        if all(s == "healthy" for s in statuses):
            health["overall"] = "healthy"
        elif any(s == "critical" for s in statuses):
            health["overall"] = "critical"
        elif any(s == "degraded" for s in statuses):
            health["overall"] = "degraded"
        else:
            health["overall"] = "unknown"

        return health

    def _check_database_health(self) -> dict:
        """Check database connectivity."""
        if self.db is None:
            return {"status": "unknown", "message": "No database connection"}

        try:
            raw_db = self.db.db if hasattr(self.db, 'db') else self.db
            # Simple ping
            raw_db.command("ping")
            return {"status": "healthy", "message": "Connected"}
        except Exception as e:
            return {"status": "critical", "message": str(e)}

    def _check_license_status(self) -> dict:
        """Check license status (placeholder)."""
        # TODO: Integrate with actual license service
        return {"status": "healthy", "message": "License check not implemented"}

    def _get_recent_errors(self, limit: int = 20) -> list:
        """
        Get recent error entries from the buffer.

        Args:
            limit: Maximum entries to return

        Returns:
            List of error entries
        """
        try:
            return error_buffer.get_recent(limit=limit)
        except Exception as e:
            logger.warning(f"Error buffer access failed: {e}")
            return []


# Module-level convenience function
def get_diagnostics(db=None, include_errors: bool = True, error_limit: int = 20) -> dict:
    """
    Convenience function to collect all diagnostics.

    Args:
        db: Database instance
        include_errors: Whether to include recent error entries
        error_limit: Maximum number of error entries

    Returns:
        Combined diagnostics dict
    """
    aggregator = DiagnosticsAggregator(db)
    return aggregator.collect_all(include_errors=include_errors, error_limit=error_limit)
