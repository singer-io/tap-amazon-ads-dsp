import json
import pytest
from tap_amazon_ads_dsp.client import AmazonAdvertisingClient


CONFIG = {
  "client_id": "CLIENT_ID",
  "client_secret": "CLIENT_SECRET",
  "refresh_token": "REFRESH_TOKEN",
  "redirect_uri": "https://redirect",
  "start_date": "2020-09-15T00:00:00Z",
  "user_agent": "tap-amazon-advertising <user@email.com>",
  "profiles": "1234567890",
  "attribution_window": "4",
  "reports": [
    {
      "name": "audience_report",
      "type": "audience"
    },
    {
      "name": "inventory_report",
      "type": "inventory"
    },
    {
      "name": "campaign_report",
      "type": "campaign"
    }
  ]
}

@pytest.fixture
def ads_client():
    ads_client = AmazonAdvertisingClient(CONFIG)
    ads_client.access_token = "ACCESS_TOKEN"
    return ads_client
