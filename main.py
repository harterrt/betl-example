from betl import *
from datetime import date, timedelta

# Setup simple table to transform:

def __main__(sc, sqlContext, day=None, save=True):
    if day is None:
        day = (date.today() - timedelta(1)).strftime("%Y%m%d")

    has_addon = lambda x: "testpilot@cliqz.com" in x.keys() if x is not None else None

    testpilottest_df = convert_pings(
        sqlContext,
        Dataset.from_source("telemetry") \
            .where(docType="testpilottest") \
            .where(submissionDate=day) \
            .where(appName="Firefox") \
            .records(sc),
        DataFrameConfig(
            [
                ("client_id", "clientId", None, StringType()),
                ("enc_cliqz_udid", "payload/payload/cliqzSession", None, StringType()),
                ("telemetry_enabled", "environment/settings/telemetryEnabled", None, BooleanType()),
                ("has_addon", "environment/addons/activeAddons", has_addon, BooleanType()),
                ("test", "payload/test", None, StringType())
            ],
            lambda ping: ping['payload/test'] == "testpilot@cliqz.com"
        )
    )

    return testpilottest_df
