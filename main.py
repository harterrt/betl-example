from moztelemetry.dataset import Dataset

testpilottest_df = pings_to_df(
    sqlContext,
    Dataset.from_source("telemetry") \
        .where(docType=docType) \
        .where(submissionDate=day) \
        .where(appName="Firefox") \
        .records(sc)
    DataFrameConfig(
        [
            ("client_id", "clientId", None, StringType()),
            ("enc_cliqz_udid", "payload/payload/cliqzSession", None, StringType()),
            ("cliqz_udid", "payload/payload/cliqzSession", decrypt_cliqz_id, StringType()),
            ("cliqz_client_id", "payload/payload/cliqzSession", split_cliqz_id, StringType()),
            ("session_id", "payload/payload/sessionId", None, StringType()),
            ("subsession_id", "payload/payload/subsessionId", None, StringType()),
            ("date", "meta/submissionDate", None, StringType()),
            ("client_timestamp", "creationDate", None, StringType()),
            ("geo", "meta/geoCountry", None, StringType()),
            ("locale", "environment/settings/locale", None, StringType()),
            ("channel", "meta/normalizedChannel", None, StringType()),
            ("os", "meta/os", None, StringType()),
            ("telemetry_enabled", "environment/settings/telemetryEnabled", None, BooleanType()),
            ("has_addon", "environment/addons/activeAddons", has_addon, BooleanType()),
            ("cliqz_version", "environment/addons/activeAddons", get_cliqz_version, StringType()),
            ("event", "payload/payload/event", None, StringType()),
            ("content_search_engine", "payload/payload/contentSearch", None, StringType()),
            ("test", "payload/test", None, StringType())
        ],
        lambda ping: ping['payload/test'] == "testpilot@cliqz.com"
    )
).filter("event IS NOT NULL")
