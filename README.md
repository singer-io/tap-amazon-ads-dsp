# tap-amazon-advertising-dsp

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Amazon Advertising DSP API, Beta](https://advertising.amazon.com/API/docs/en-us/dsp-reports-beta-3p/#/Reports)
- Extracts Asyncronous Reports:
  - Supports report types: Audience, Campaign, Inventory
  - **Profile**: Reports are configured by Entity(Amazon Ads profile id)
    - Profiles/entities may be discovered via https://advertising-api.amazon.com/v2/profiles
    - A list of entities may be configured
  - async_results (download URL)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## DSP API Setup and Access

In order to use the Amazon Advertising DSP API the following high-level steps must be completed.

### Account Setup

[Account setup documentation](https://advertising.amazon.com/API/docs/en-us/setting-up/account-setup). This creates the developer account whose credentials will be used later to generate the required refresh token used for the Tap configuration.

### Advertising API application

[Advertising API Application documentation](https://advertising.amazon.com/about-api). This application will generate an initial email requesting further information. And finally, an approval email will be received. This email will detail the next steps and is required for the next section, Additional Setup Steps for the DSP API.

### Additional Setup Steps for the DSP API

[Additional steps documentation](https://advertising.amazon.com/API/docs/en-us/setting-up/dsp). This document describes the additional setup steps required to onboard an application for use with the DSP API. Once the approval email is received following the directions and those documented here.

### API Authorization and refresh tokens

[Create API Authorization and Refresh Token](https://advertising.amazon.com/API/docs/en-us/setting-up/generate-api-tokens), this document describes the steps required to generate the authorization and refresh tokens.

## Authentication
OAuth is the required method of authenticating. The Amazon Advertising API manages permissions using the Login with Amazon service. The API uses authorization and refresh tokens in a standard OAuth 2.0 flow. Further documentation available here.

The refresh token once generated is permanent, but access tokens are short-lived. The tap manages refreshing access tokens throughout the sync lifecycle.


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
      "profiles": ["2389773460286997, 3393509102664206"],
      "attribution_window": "14",
      "reports": [
        {
          "name": "inventory_report",
          "type": "inventory"
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
    Checking stdin for valid Singer-formatted data
    The output is valid.
    It contained 1225 messages for 3 streams.

          3 schema messages
      1198 record messages
        24 state messages

    Details by stream:
    +------------------+---------+---------+
    | stream           | records | schemas |
    +------------------+---------+---------+
    | inventory_report | 378     | 1       |
    | audience_report  | 788     | 1       |
    | campaign_report  | 32      | 1       |
    +------------------+---------+---------+
    ```
---

Copyright &copy; 2019 Stitch
