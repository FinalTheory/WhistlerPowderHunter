import unittest
from unittest.mock import Mock, patch

from context.whistler import fetch_snow_history


class SnowHistoryTests(unittest.TestCase):
    def test_daily_snow_and_base_preserve_zero_and_missing_values(self):
        def row(day, snow_class, snow, base):
            return (
                f"<div class='day-container'><div class='dayData'>{day}</div>"
                f"<div class='{snow_class}'>{snow}<span class='units'>cm</span></div>"
                "<div class='seasonData'>805<span class='units'>cm</span></div>"
                f"<div class='baseData'>{base}<span class='units'>cm</span></div></div>"
            )

        response = Mock(text="".join([
            row("Nov 22", "snowData", "3", "36"),
            row("Nov 21", "units", "0", "36"),
            row("Nov 20", "units", "", "37"),
            row("Nov 19", "units", "", ""),
        ]))
        with patch("context.whistler.requests.get", return_value=response):
            self.assertEqual(fetch_snow_history(), [
                "Nov 22: 3cm new snow, 36cm base",
                "Nov 21: 0cm new snow, 36cm base",
                "Nov 20: Unavailable new snow, 37cm base",
            ])
