import argparse
import logging
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

import httpx
from rich.console import Console
from rich.table import Table


class PullpushFetcher:
    def __init__(self, api_base_url: str = "https://api.pullpush.io/reddit"):
        """
        Initialize the PushshiftFetcher with a configurable API base URL.

        Args:
            api_base_url (str): Base URL for the Pushshift API
        """
        self.api_base_url = api_base_url.rstrip("/")
        self.client = httpx.Client(timeout=30.0)

        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def _get_epoch_timestamp(self, year: int, start: bool = True) -> int:
        """Convert year to epoch timestamp for start or end of year."""
        if start:
            return int(datetime(year, 1, 1).timestamp())
        return int(datetime(year + 1, 1, 1).timestamp())

    def fetch_top_posts(
        self,
        subreddit: str,
        year: int,
        limit: int = 100,
        score_threshold: Optional[int] = None,
        fields: List[str] = None,
    ) -> List[Dict]:
        """
        Fetch top posts from a subreddit for a given year.

        Args:
            subreddit (str): Name of the subreddit
            year (int): Year to fetch posts from
            limit (int): Maximum number of posts to fetch
            score_threshold (int, optional): Minimum score threshold for posts
            fields (List[str], optional): Specific fields to retrieve

        Returns:
            List[Dict]: List of posts matching the criteria
        """
        if fields is None:
            fields = [
                "title",
                "score",
                "url",
                "author",
                "created_utc",
                "full_link",
                "selftext",
            ]

        params = {
            "subreddit": subreddit,
            "size": min(limit, 100),  # API typically limits to 100 per request
            "after": self._get_epoch_timestamp(year),
            "before": self._get_epoch_timestamp(year, False),
            "sort": "desc",
            "sort_type": "score",
        }

        if score_threshold:
            params["score"] = f">{score_threshold}"

        endpoint = f"{self.api_base_url}/search/submission"
        all_posts = []

        try:
            while len(all_posts) < limit:
                self.logger.info(
                    f"Fetching posts {len(all_posts)} to {len(all_posts) + params['size']}"
                )

                response = self.client.get(endpoint, params=params)
                response.raise_for_status()

                data = response.json().get("data", [])
                if not data:
                    break

                all_posts.extend(data)

                # Update parameters for next batch
                last_created = data[-1]["created_utc"]
                params["before"] = last_created

                if len(all_posts) < limit:
                    # Respect API rate limits
                    time.sleep(1)

                if len(data) < params["size"]:  # No more results available
                    break

            return all_posts[:limit]

        except httpx.HTTPError as e:
            self.logger.error(f"Error fetching posts: {str(e)}")
            raise


def display_posts(posts: List[Dict], console: Console):
    """Display posts in a formatted table using rich."""
    table = Table(show_header=True, header_style="bold blue")
    table.add_column("#", style="dim", width=4)
    table.add_column("Title", width=60)
    table.add_column("Score", justify="right", width=10)
    table.add_column("Post Link")
    table.add_column("URL")

    for idx, post in enumerate(posts, 1):
        table.add_row(
            str(idx),
            post["title"],
            str(post["score"]),
            f"https://reddit.com{post['permalink']}",
            post["url"],
        )

    console.print(table)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch top posts from a subreddit for a given year"
    )
    parser.add_argument("subreddit", help="Name of the subreddit")
    parser.add_argument("year", type=int, help="Year to fetch posts from")
    parser.add_argument(
        "-n",
        "--limit",
        type=int,
        default=10,
        help="Number of posts to fetch (default: 10)",
    )
    parser.add_argument("--min-score", type=int, help="Minimum score threshold")
    parser.add_argument(
        "--api-url",
        default="https://api.pullpush.io/reddit",
        help="Alternative Pushshift API URL",
    )

    args = parser.parse_args()
    console = Console()

    try:
        with PullpushFetcher(args.api_url) as fetcher:
            posts = fetcher.fetch_top_posts(
                subreddit=args.subreddit,
                year=args.year,
                limit=args.limit,
                score_threshold=args.min_score,
                fields=["title", "score", "created_utc", "full_link"],
            )

            if not posts:
                console.print("[red]No posts found matching the criteria.[/red]")
                return

            console.print(
                f"\n[green]Top {len(posts)} posts from r/{args.subreddit} in {args.year}:[/green]\n"
            )
            display_posts(posts, console)

    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
