# tap-amazon-advertising-dsp

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Amazon Advertising DSP API, Beta](https://advertising.amazon.com/API/docs/en-us/dsp-reports-beta-3p/#/Reports)
- Extracts Asyncronous Reports using:
  - Supports many reports, each with Report Config Settings:
    - **Entity**: Reports are selected by entity id(Amazon Ads profile id)
    - **Dimensions**: 5 dimensions types(ORDER, LINE_ITEM, CREATIVE, SITE, SUPPLY)
  - async_results (download URL)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Authentication
Amazon Advertising requires authentication headers using OAuth 2 with an access token obtained via 3-legged OAuth flow. The access token, once generated, is permanent, but request tokens are short-lived without a documented expiry.


## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-amazon-ads-dsp
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. 

    ```json
    {
  "client_id": "",
  "client_secret": "",
  "refresh_token": "",
  "redirect_uri": "",
  "start_date": "2020-07-01T00:00:00Z",
  "user_agent": "tap-amazon-advertising <user@email.com>",
  "entities": "2389773460286997, 3393509102664206",
  "attribution_window": "14",
  "reports": [
    {
      "name": "inventory_report",
      "type": "inventory",
    },
    {
      "name": "campaign_report",
      "type": "campaign",
    },
    {
      "name": "audience_report",
      "type": "audience"
    }
  ]
}
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
    "bookmarks": {
        "campaign_report": {
            "2389773460286997": "20200705",
            "3393509102664206": "20200705"
        },
        "inventory_report": {
            "2389773460286997": "20200705",
            "3393509102664206": "20200705"
        },
        "audience_report": {
            "2389773460286997": "20200703",
            "3393509102664206": "20200703"
        }
    },
    "currently_syncing": "audience_report"
}
    ```

1. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-amazon-ads-dsp --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

2. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-amazon-ads-dsp --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-amazon-ads-dsp --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-amazon-ads-dsp --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

3. Test the Tap
    
    While developing the AMAZON-ADVERTISING-DSP tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_amazon-advertising-dsp -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.83/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-amazon-advertising-dsp --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 8240 messages for 16 streams.

        16 schema messages
    8108 record messages
        116 state messages

    Details by stream:
    +-----------------------------+---------+---------+
    | stream                      | records | schemas |
    +-----------------------------+---------+---------+
    | **ENDPOINT_A**              | 23      | 1       |
    +-----------------------------+---------+---------+
    ```
---

Copyright &copy; 2019 Stitch
