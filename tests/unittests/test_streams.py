import unittest
import pytz
from tap_jira.context import Context
from unittest.mock import Mock, MagicMock, patch
from tap_jira.streams import Issues
from tap_jira.http import EnhancedSearchPaginator
from datetime import datetime

class TestLocalizedRequests(unittest.TestCase):
    def setUp(self):
        self.tzname = 'Europe/Volgograd'
        Context.update_start_date_bookmark = Mock(return_value=datetime(2018,12,12,1,2,3, tzinfo=pytz.UTC))
        Context.retrieve_timezone = Mock(return_value=self.tzname)
        Context.bookmark = Mock()
        Context.set_bookmark = Mock()
        Context.catalog = Mock()
        
        # Mock the catalog entry structure
        mock_schema = Mock()
        mock_schema.properties = {
            'fields': Mock(properties={})
        }
        mock_catalog_entry = Mock()
        mock_catalog_entry.schema = mock_schema
        Context.get_catalog_entry = Mock(return_value=mock_catalog_entry)
        
        # Mock the client
        mock_client = Mock()
        mock_client.request = Mock(return_value=[])  # Return empty list for field requests
        Context.client = mock_client
        
        # Mock the config
        Context.config = Mock()
        Context.config.get = Mock(return_value=None)  # Return None for projects config
        
        # Mock other Context methods
        Context.get_projects = Mock(return_value=[])

    @patch.object(EnhancedSearchPaginator, 'pages')
    def test_issues_local_timezone_in_request(self, mock_pages):
        mock_pages.return_value = iter([])  # Empty iterator for pages
        
        issues = Issues('issues', ['pk_fields'])
        issues.sync()

        user_tz = pytz.timezone(self.tzname)
        expected_start_date = (datetime(2018, 12, 12, 1, 2, tzinfo=pytz.UTC)
                               .astimezone(user_tz)
                               .strftime("%Y-%m-%d %H:%M"))
        expected_jql = "updated >= '{}' order by updated asc".format(expected_start_date)
        mock_pages.assert_called_once_with('issues', expected_jql, fields=['*all'], expand=['changelog', 'transitions'])
