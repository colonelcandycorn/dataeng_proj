from src.data_quality_tester import DataQualityTester
from src.postgres_connector import PostgresConnector


if __name__ == "__main__":
    #get breadcrumb and trip dataframes
    connector = PostgresConnector()
    breadcrumb_df = connector.get_breadcrumb()
    trip_df = connector.get_trip()

    #initialize the data quality tester
    tester = DataQualityTester()

    #run the tests
    tester.test_unique_col(trip_df, 'trip_id', 'trip')
    tester.test_duplicates_rows(trip_df, 'trip')
    tester.test_for_negative_values(trip_df, 'trip_id', 'trip')
    tester.test_unique_col(breadcrumb_df, 'trip_id', 'breadcrumb')
    tester.test_duplicates_rows(breadcrumb_df, 'breadcrumb')
    tester.test_for_negative_values(breadcrumb_df, 'speed', 'breadcrumb')
    tester.test_for_missing_values(trip_df, 'trip_id', 'trip')
    tester.test_for_missing_values(breadcrumb_df, 'trip_id', 'breadcrumb')
    tester.test_for_missing_values(breadcrumb_df, 'speed', 'breadcrumb')
    tester.test_for_missing_values(trip_df, 'vehicle_id', 'trip')
    tester.test_for_missing_values(breadcrumb_df, 'tstamp', 'breadcrumb')

    # print the results
    tester.pretty_print_results()

