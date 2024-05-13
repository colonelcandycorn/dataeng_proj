import os
import datetime as dt
import pandas as pd


class BreadCrumbProcessor:
    """
    This class does most of the validation and processing of the breadcrumb data. It contains methods to process
    individual breadcrumbs, process a batch of breadcrumbs, and convert the raw breadcrumb data into processed trip and
    breadcrumb tables. The processed tables are then appended to the Postgres database.
    """
    def process_ndjson_files(file_path: str) -> pd.DataFrame | None:
        """
        Process a ndjson file and return a cleaned dataframe (note that this doesn't check for nulls). After part 1 of
        the project, I was storing the data in ndjson files, so I created this method to process the huge backlog of
        data I had. This function is no longer being used.
        :param file_path: str path to the ndjson file that needs to be processed
        :return: a cleaned dataframe or None if the file doesn't exist
        """
        if not os.path.exists(file_path):
            return None

        if file_path.endswith(".ndjson"):
            bc_df = pd.read_json(file_path, lines=True)
            bc_df = BreadCrumbProcessor.process_batch(bc_df)
            bc_df = bc_df.drop_duplicates()
            return bc_df

    def process_batch(breadcrumb_batch: pd.DataFrame) -> pd.DataFrame | None:
        """
        Process a batch of breadcrumbs. This is just a helper method for the process_ndjson_files method. It is no
        longer being used.
        :return: a cleaned dataframe or None if the input dataframe is invalid
        """
        # remove rows with gps_hdop > 20
        clean_df = breadcrumb_batch[breadcrumb_batch["GPS_HDOP"] <= 20]

        # remove rows with negative meters, act_time, or vehicle_id
        for col in ["METERS", "ACT_TIME", "VEHICLE_ID"]:
            clean_df = clean_df[clean_df[col] >= 0]

        # drop unnecessary columns
        clean_df = clean_df.drop(["EVENT_NO_STOP", "GPS_HDOP", "GPS_SATELLITES"], axis=1)

        # add proessed date
        clean_df = BreadCrumbProcessor.add_processed_date(clean_df)

        # add timestamp
        clean_df = BreadCrumbProcessor.add_timestamp(clean_df)

        return clean_df

    def process_individual(breadcrumb: dict) -> pd.DataFrame | None:
        """
        This is one of the most important methods in this class. It processes individual breadcrumbs and returns
        a cleaned version of the breadcrumb. I chose to return a dataframe even though it only contains one row because
        it makes it easier (for me) to work with the data in the rest of the code.
        :param breadcrumb: dict a single breadcrumb that is read from the PubSub message
        :return: a cleaned dataframe or None if the input breadcrumb is invalid
        """
        # read breadcrumb string
        bc_df = pd.DataFrame([breadcrumb])

        # add processed date
        bc_df = BreadCrumbProcessor.add_processed_date(bc_df)

        # clean the dataframe
        bc_df = BreadCrumbProcessor.clean_breadcrumb(bc_df)

        if bc_df is None:
            return None

        # add timestamp
        bc_df = BreadCrumbProcessor.add_timestamp(bc_df)

        return bc_df

    def _create_timestamp(row: pd.Series) -> dt.datetime:
        """
        Most of this logic was taken from the in class exercise. We take the date column, OPD_DATE, that is given in a
        specific date time format and transform it into an actual datetime object. We then take the time delta column,
        ACT_TIME, which is given in seconds since midnight, and transform it into a timedelta object. We then add the
        two together to get the timestamp.
        :param row: pd.Series a single row of the breadcrumb dataframe
        :return: dt.datetime the timestamp of the breadcrumb
        """
        dt_str = row["OPD_DATE"]
        time_delta = row["ACT_TIME"]
        date = dt.datetime.strptime(dt_str, "%d%b%Y:%H:%M:%S")
        time_delta = dt.timedelta(seconds=time_delta)
        return date + time_delta

    def add_timestamp(row: pd.DataFrame) -> pd.DataFrame | None:
        """
        This method uses apply and the helper method _create_timestamp to add a timestamp column to the breadcrumb
        dataframe. I chose to use apply because it is faster than iterating through the dataframe with a for loop.
        :param row: pd.DataFrame the breadcrumb dataframe
        :return: a dataframe with a timestamp column or None if the input dataframe is invalid
        """
        # create timestamp column
        try:
            row["timestamp"] = row.apply(BreadCrumbProcessor._create_timestamp, axis=1)
        except Exception:
            return None

        # drop OPD_DATE and ACT_TIME columns
        ts_breadcrumb = row.drop(["OPD_DATE", "ACT_TIME"], axis=1)

        return ts_breadcrumb

    def add_processed_date(row: pd.DataFrame) -> pd.DataFrame:
        """
        Add a processed date column to the breadcrumb dataframe. Since the bus data contains dates from the past, I
        thought that it might be useful to add a column that contains the date the data was processed. This way, if I
        process data from multiple days, I can easily filter the data by the date it was processed.

        I ended up not using this column in the final data processing, but I thought it was a good idea to keep it in
        case I needed it in the future.
        :param row: pd.DataFrame I call it row because I assumed that this would be used in conjunction with the
        process_individual method, which processes one row at a time.
        :return: df with processed_date column
        """
        today = dt.date.today()
        row["processed_date"] = today
        row["is_in_final_table"] = False
        return row

    def clean_breadcrumb(breadcrumb: pd.DataFrame) -> pd.Series | None:
        """
        For part 2 of the project, we were tasked with implementing 10 validations/transformations. I implemented the
        following validations/transformations: all columns from specification are present, no nulls are present in any
        column/row, meters, act_time, and vehicle_id are all positive, and gps_hdop is less than 20. I also implemented
        a transformation to drop unnecessary columns.
        :param breadcrumb: pd.DataFrame a single breadcrumb that is read from the PubSub message
        :return: a cleaned dataframe or None if the input breadcrumb is invalid
        """
        # Verify that the breadcrumb has the necessary columns
        if not all(col in breadcrumb.columns for col in
                   ["EVENT_NO_TRIP", "OPD_DATE", "VEHICLE_ID", "METERS", "ACT_TIME", "GPS_LONGITUDE", "GPS_LATITUDE",
                    "GPS_HDOP", "GPS_SATELLITES"]):
            return None

        # verify there are no nulls in the breadcrumb
        if breadcrumb.isnull().values.any():
            return None

        # if the GPS_HDOP is greater than 20, discard the breadcrumb as it is likely to be inaccurate
        if breadcrumb["GPS_HDOP"].max() > 20:
            return None

        for col in ["METERS", "ACT_TIME", "VEHICLE_ID"]:
            if (breadcrumb[col] < 0).any():
                return None

        # drop unnecessary columns
        clean_breadcrumb = breadcrumb.drop(["EVENT_NO_STOP", "GPS_HDOP", "GPS_SATELLITES"], axis=1)

        return clean_breadcrumb

    def add_speed(df: pd.DataFrame) -> pd.DataFrame:
        """
        This method is also taken directly from the in class exercise. It calculates the speed of the bus by taking the
        difference in meters and dividing it by the difference in time. Currently there is no validation to check if the
        speed is within a reasonable range. In theory, the previous validations ensure that at least the meters and time
        are positive, so the speed should be positive as well. Removing the high GPS_HDOP values should also help
        ensure that the values produced by this method are more accurate. However, using my other class,
        DataQualityTester, I have found about 500 rows out of 6.5 million that have a speed greater than 30 m/s which
        is about 67 mph. I don't have any hard data but I think that anything over 67 mph for a bus is probably
        inaccurate.
        :param df: pd.DataFrame the breadcrumb dataframe that has "METERS" and "ACT_TIME" columns
        :return: pd.DataFrame the breadcrumb dataframe with a "speed" column
        """
        df = df.sort_values(["EVENT_NO_TRIP", "VEHICLE_ID", "timestamp"])

        # create delta meters column
        df["dMETERS"] = df.groupby(["EVENT_NO_TRIP", "VEHICLE_ID"])["METERS"].diff()

        # create delta timestamp column
        df["dTIMESTAMP"] = df.groupby(["EVENT_NO_TRIP", "VEHICLE_ID"])["timestamp"].diff()

        # create speed column
        df["speed"] = df.apply(
            lambda row: row["dMETERS"] / row["dTIMESTAMP"].total_seconds(),
            axis=1)
        # drop unnecessary columns
        return df.drop(["dMETERS", "dTIMESTAMP"], axis=1)

    def raw_table_to_processed_tables(df: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
        """
        This method is an artifact from when I was still using a data lake style architecture. I would store the
        slightly processed breadcrumbs in a 'raw dataframe' and then process the raw dataframe into a trip table and a
        breadcrumb table. I would then append the trip and breadcrumb tables to the Postgres database. I have since
        just moved this work into the subscriber class. I have kept this method in case I need to use it in the future.
        :param df: pd.DataFrame the raw breadcrumb dataframe
        :return: two dataframes, the trip table and the breadcrumb table (currently the trip table is mostly null)
        """
        # add speed
        format_df = BreadCrumbProcessor.add_speed(df)

        format_df = format_df.rename(columns={"timestamp": "tstamp", "GPS_LATITUDE": "latitude", "GPS_LONGITUDE": "longitude",
                                "EVENT_NO_TRIP": "trip_id", "VEHICLE_ID": "vehicle_id"})

        # set up trip table
        trip_table = format_df[["trip_id", "vehicle_id"]].drop_duplicates()
        # add null columns route_id, direction, and service_key
        trip_table["route_id"] = None
        trip_table["direction"] = None
        trip_table["service_key"] = None

        # reorder columns
        trip_table = trip_table[["trip_id", "route_id", "vehicle_id", "service_key", "direction"]]

        # set up breadcrumb table
        breadcrumb_table = format_df[["tstamp", "latitude", "longitude", "speed", "trip_id"]]

        return trip_table, breadcrumb_table

