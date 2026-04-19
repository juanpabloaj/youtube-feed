from youtube_feed.models import SummaryResult


def should_notify(summary: SummaryResult, min_notification_score: int) -> bool:
    return summary.priority.lower() == "high" and summary.score >= min_notification_score
