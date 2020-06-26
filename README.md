# tap-amazon-advertising-dsp

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [**CLIENT** API](xxx)
- Extracts the following resources:
  - **TBD**
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams

[**ENDPOINT_A**](**URL**)
- Endpoint: **URL**
- Primary key fields: 
- Foreign key fields: 
- Replication strategy: INCREMENTAL (query filtered)
  - Bookmark query fields:
  - Bookmark: ex: modified (date-time)
- Transformations: none

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-amazon-advertising-dsp
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

3. Create your tap's `config.json` file. The `api_sub_domain` is everything before `.amazon-advertising-dsp.com.` in the AMAZON-ADVERTISING-DSP URL.  The `account_name` is everything between `..com.` and `api` in the AMAZON-ADVERTISING-DSP URL. The `date_window_size` is the integer number of days (between the from and to dates) for date-windowing through the date-filtered endpoints (default = 60).

    ```json
    {
        "token": "YOUR_API_TOKEN",
        "account_name": "YOUR_ACCOUNT_NAME",
        "server_subdomain": "YOUR_SERVER_SUBDOMAIN",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-amazon-advertising-dsp <api_user_email@your_company.com>",
        "date_window_size": "60"
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "registers",
        "bookmarks": {
            "customers": "2019-06-11T13:37:51Z",
            "contracts": "2019-06-19T19:48:42Z",
            "invoices": "2019-06-18T18:23:53Z",
            "items": "2019-06-20T00:52:44Z",
            "transactions": "2019-06-19T19:48:45Z",
            "registers": "2019-06-11T13:37:56Z",
            "revenue_entries": "2019-06-19T19:48:47Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-amazon-advertising-dsp --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-amazon-advertising-dsp --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-amazon-advertising-dsp --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-amazon-advertising-dsp --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
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
