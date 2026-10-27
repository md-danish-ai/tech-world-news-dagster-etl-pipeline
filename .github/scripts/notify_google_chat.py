#!/usr/bin/env python3
"""
GitHub Actions to Google Chat Notification Script

Handles:
1. Direct pushes to branches (push event) - added before PR conditions
2. Pull requests (opened, reopened, merged, closed)
3. Pull request reviews (approvals, changes requested)
"""

import json
import os
import sys
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.request


def escape_chat(text: str) -> str:
    """Escape special characters for Google Chat text messages."""
    if not text:
        return ""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def send_google_chat_message(webhook_url: str, text: str) -> bool:
    """Send a formatted text payload to a Google Chat incoming webhook."""
    payload = {"text": text}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json; charset=UTF-8"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            status = resp.getcode()
            response_body = resp.read().decode("utf-8")
            print(f"Google Chat response [{status}]: {response_body}")
            return True
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        print(f"HTTPError sending to Google Chat [{e.code}]: {error_body}", file=sys.stderr)
        return False
    except urllib.error.URLError as e:
        print(f"URLError connecting to Google Chat: {e.reason}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Unexpected error sending message: {e}", file=sys.stderr)
        return False


def build_message(event_name: str, event: Dict[str, Any]) -> Optional[str]:
    """Build the notification message depending on the GitHub event."""
    repo = event.get("repository", {})
    repo_name = repo.get("name") or repo.get("full_name", "Repository")
    repo_url = repo.get("html_url", "")
    repo_link = f"<{repo_url}|{escape_chat(repo_name)}>" if repo_url else f"*{escape_chat(repo_name)}*"

    # ------------------------------------------------------------------
    # 1. PUSH EVENT CONDITION (Direct Pushes to a Branch)
    #    Placed before PR conditions as required.
    # ------------------------------------------------------------------
    if event_name == "push":
        # Check if the branch was deleted
        if event.get("deleted", False):
            ref = event.get("ref", "")
            branch = ref.replace("refs/heads/", "")
            pusher = event.get("pusher", {}).get("name") or event.get("sender", {}).get("login", "Someone")
            return f"🗑️ *Branch Deleted:* `{escape_chat(branch)}` in {repo_link}\n• *Deleted by:* `{escape_chat(pusher)}`"

        ref = event.get("ref", "")
        # Detect if it's a branch or tag
        if ref.startswith("refs/heads/"):
            branch = ref.replace("refs/heads/", "")
            ref_type = "Branch"
        elif ref.startswith("refs/tags/"):
            branch = ref.replace("refs/tags/", "")
            ref_type = "Tag"
        else:
            branch = ref
            ref_type = "Ref"

        pusher = event.get("pusher", {}).get("name") or event.get("sender", {}).get("login", "Unknown")
        commits = event.get("commits", [])
        commit_count = len(commits)
        compare_url = event.get("compare", "")
        forced = event.get("forced", False)

        force_text = " (Force Push)" if forced else ""
        header = f"🚀 *Push to {escape_chat(branch)}*{force_text} in {repo_link}"

        lines = [
            header,
            f"• *Pushed by:* `{escape_chat(pusher)}`",
        ]

        # Display only the latest commit for this push
        latest_commit = event.get("head_commit") or (commits[-1] if commits else None)
        if latest_commit:
            sha = latest_commit.get("id", "")[:7]
            raw_msg = latest_commit.get("message", "").split("\n")[0].strip()
            msg = escape_chat(raw_msg)
            author = latest_commit.get("author", {}).get("name") or latest_commit.get("author", {}).get("username", "")
            url = latest_commit.get("url", "")

            commit_display = f"<{url}|`{sha}`>" if url else f"`{sha}`"
            author_display = f" - _{escape_chat(author)}_" if author else ""
            lines.append(f"• *Latest commit:* {commit_display} {msg}{author_display}")

        if compare_url:
            lines.append(f"• <{compare_url}|View Changes on GitHub>")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # 2. PULL REQUEST EVENT CONDITIONS (Opened, Merged, Closed)
    # ------------------------------------------------------------------
    elif event_name == "pull_request":
        action = event.get("action")
        pr = event.get("pull_request", {})
        pr_number = pr.get("number")
        pr_title = escape_chat(pr.get("title", "No Title"))
        pr_url = pr.get("html_url", "")
        pr_link = f"<{pr_url}|#{pr_number} {pr_title}>" if pr_url else f"#{pr_number} {pr_title}"
        author = pr.get("user", {}).get("login", "Unknown")
        base_branch = escape_chat(pr.get("base", {}).get("ref", ""))
        head_branch = escape_chat(pr.get("head", {}).get("ref", ""))

        if action in ("opened", "reopened"):
            verb = "Opened" if action == "opened" else "Reopened"
            lines = [
                f"📬 *Pull Request {verb}* in {repo_link}",
                f"• *PR:* {pr_link}",
                f"• *Author:* `{escape_chat(author)}`",
                f"• *Branches:* `{head_branch}` ➔ `{base_branch}`",
            ]
            return "\n".join(lines)

        elif action == "closed":
            merged = pr.get("merged", False)
            if merged:
                merged_by = pr.get("merged_by", {}).get("login") or event.get("sender", {}).get("login", "Unknown")
                lines = [
                    f"🎉 *Pull Request Merged!* in {repo_link}",
                    f"• *PR:* {pr_link}",
                    f"• *Merged by:* `{escape_chat(merged_by)}`",
                    f"• *Target Branch:* `{base_branch}` (from `{head_branch}`)",
                ]
                return "\n".join(lines)
            else:
                closed_by = event.get("sender", {}).get("login", "Unknown")
                lines = [
                    f"🚫 *Pull Request Closed (Unmerged)* in {repo_link}",
                    f"• *PR:* {pr_link}",
                    f"• *Closed by:* `{escape_chat(closed_by)}`",
                ]
                return "\n".join(lines)

        else:
            print(f"Skipping PR action '{action}' (not opened, reopened, or closed).")
            return None

    # ------------------------------------------------------------------
    # 3. PULL REQUEST REVIEW CONDITIONS (Approvals & Changes Requested)
    # ------------------------------------------------------------------
    elif event_name == "pull_request_review":
        action = event.get("action")
        review = event.get("review", {})
        review_state = review.get("state", "").lower()
        reviewer = review.get("user", {}).get("login", "Unknown")

        pr = event.get("pull_request", {})
        pr_number = pr.get("number")
        pr_title = escape_chat(pr.get("title", "No Title"))
        pr_url = pr.get("html_url", "")
        pr_link = f"<{pr_url}|#{pr_number} {pr_title}>" if pr_url else f"#{pr_number} {pr_title}"
        pr_author = pr.get("user", {}).get("login", "Unknown")

        if review_state == "approved":
            lines = [
                f"✅ *Pull Request Approved!* in {repo_link}",
                f"• *PR:* {pr_link}",
                f"• *Approved by:* `{escape_chat(reviewer)}`",
                f"• *PR Author:* `{escape_chat(pr_author)}`",
            ]
            review_body = (review.get("body") or "").strip()
            if review_body:
                # Truncate comment if very long
                comment_preview = review_body if len(review_body) <= 150 else review_body[:147] + "..."
                lines.append(f"• *Comment:* _{escape_chat(comment_preview)}_")
            return "\n".join(lines)

        elif review_state == "changes_requested":
            lines = [
                f"⚠️ *Changes Requested on Pull Request* in {repo_link}",
                f"• *PR:* {pr_link}",
                f"• *Requested by:* `{escape_chat(reviewer)}`",
                f"• *PR Author:* `{escape_chat(pr_author)}`",
            ]
            review_body = review.get("body", "").strip()
            if review_body:
                comment_preview = review_body if len(review_body) <= 150 else review_body[:147] + "..."
                lines.append(f"• *Comment:* _{escape_chat(comment_preview)}_")
            return "\n".join(lines)

        else:
            print(f"Skipping review state '{review_state}' (only notifying on approvals/changes_requested).")
            return None

    else:
        print(f"Event '{event_name}' is not configured for notifications.")
        return None


def main():
    webhook_url = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        print("ERROR: GOOGLE_CHAT_WEBHOOK_URL environment variable is missing or empty.", file=sys.stderr)
        print("Please verify that GOOGLE_CHAT_WEBHOOK_URL is configured in GitHub Repository Secrets.")
        # Exit with 0 so workflows don't fail if secrets are absent (e.g. forked PRs)
        sys.exit(0)

    event_name = os.environ.get("GITHUB_EVENT_NAME", "").strip()
    event_path = os.environ.get("GITHUB_EVENT_PATH", "").strip()

    if not event_path or not os.path.exists(event_path):
        print(f"ERROR: GITHUB_EVENT_PATH '{event_path}' does not exist or is not set.", file=sys.stderr)
        sys.exit(1)

    with open(event_path, "r", encoding="utf-8") as f:
        event = json.load(f)

    print(f"Processing GitHub event '{event_name}'...")
    message = build_message(event_name, event)

    if not message:
        print("No notification required for this event.")
        return

    print("Generated message payload:\n" + message)
    success = send_google_chat_message(webhook_url, message)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
