import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    """
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    """

    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        orders = self.data["orders"].copy()

        # filter
        if is_delivered:
            orders = orders.query("order_status == 'delivered'").copy()

        # convert to datetime
        datetime_columns = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]
        orders.loc[:, datetime_columns] = orders[datetime_columns].apply(pd.to_datetime)

        # set variables
        purchase_date = orders["order_purchase_timestamp"]
        delivery_date = orders["order_delivered_customer_date"]
        expected_delivery_date = orders["order_estimated_delivery_date"]

        # utility function
        def calc_day_delta(series1, series2):
            return (series1 - series2) / pd.Timedelta(days=1)

        # calculate wait times
        orders.loc[:, "wait_time"] = calc_day_delta(delivery_date, purchase_date)
        orders.loc[:, "expected_wait_time"] = calc_day_delta(
            expected_delivery_date, purchase_date
        )

        # compare
        wait_time = orders["wait_time"]
        expected_wait_time = orders["expected_wait_time"]
        orders.loc[:, "delay_vs_expected"] = np.maximum(
            wait_time - expected_wait_time, 0
        )

        # return columns
        return orders[
            [
                "order_id",
                "wait_time",
                "expected_wait_time",
                "delay_vs_expected",
                "order_status",
            ]
        ]

    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        reviews = self.data["order_reviews"].copy()
        reviews.loc[:, "dim_is_five_star"] = (reviews["review_score"] == 5).astype(
            "int"
        )
        reviews.loc[:, "dim_is_one_star"] = (reviews["review_score"] == 1).astype("int")

        return reviews[
            ["order_id", "dim_is_five_star", "dim_is_one_star", "review_score"]
        ]

    def get_number_items(self):
        """
        Returns a DataFrame with:
        order_id, number_of_items
        """
        order_items = self.data["order_items"].copy()
        number_of_items = order_items.groupby("order_id")["order_item_id"].count()

        return number_of_items.reset_index().rename(
            columns={"order_item_id": "number_of_items"}
        )

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        order_items = self.data["order_items"].copy()
        number_of_sellers = order_items.groupby("order_id")["seller_id"].nunique()

        return number_of_sellers.reset_index().rename(
            columns={"seller_id": "number_of_sellers"}
        )

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        order_items = self.data["order_items"].copy()
        price_and_freight = order_items.groupby("order_id")[
            ["price", "freight_value"]
        ].sum()

        return price_and_freight.reset_index()

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        # $CHALLENGIFY_BEGIN

        # import data
        data = self.data
        orders = data["orders"]
        order_items = data["order_items"]
        sellers = data["sellers"]
        customers = data["customers"]

        # Since one zip code can map to multiple (lat, lng), take the first one
        geo = data["geolocation"]
        geo = geo.groupby("geolocation_zip_code_prefix", as_index=False).first()

        # Merge geo_location for sellers
        sellers_mask_columns = [
            "seller_id",
            "seller_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
        ]

        sellers_geo = sellers.merge(
            geo,
            how="left",
            left_on="seller_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
        )[sellers_mask_columns]

        # Merge geo_location for customers
        customers_mask_columns = [
            "customer_id",
            "customer_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
        ]

        customers_geo = customers.merge(
            geo,
            how="left",
            left_on="customer_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
        )[customers_mask_columns]

        # Match customers with sellers in one table
        customers_sellers = (
            customers.merge(orders, on="customer_id")
            .merge(order_items, on="order_id")
            .merge(sellers, on="seller_id")[
                [
                    "order_id",
                    "customer_id",
                    "customer_zip_code_prefix",
                    "seller_id",
                    "seller_zip_code_prefix",
                ]
            ]
        )

        # Add the geoloc
        matching_geo = customers_sellers.merge(sellers_geo, on="seller_id").merge(
            customers_geo, on="customer_id", suffixes=("_seller", "_customer")
        )
        # Remove na()
        matching_geo = matching_geo.dropna()

        matching_geo.loc[:, "distance_seller_customer"] = matching_geo.apply(
            lambda row: haversine_distance(
                row["geolocation_lng_seller"],
                row["geolocation_lat_seller"],
                row["geolocation_lng_customer"],
                row["geolocation_lat_customer"],
            ),
            axis=1,
        )
        # Since an order can have multiple sellers,
        # return the average of the distance per order
        order_distance = matching_geo.groupby("order_id", as_index=False).agg(
            {"distance_seller_customer": "mean"}
        )

        return order_distance

    def get_training_data(self, is_delivered=True, with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_items', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        wait_time = self.get_wait_time(is_delivered)
        review_score = self.get_review_score()
        get_number_items = self.get_number_items()
        get_number_sellers = self.get_number_sellers()
        get_price_and_freight = self.get_price_and_freight()

        training_data = wait_time
        dfs_to_merge = [
            review_score,
            get_number_items,
            get_number_sellers,
            get_price_and_freight,
        ]
        for df in dfs_to_merge:
            training_data = training_data.merge(df, on="order_id")

        if with_distance_seller_customer:
            get_distance_seller_customer = self.get_distance_seller_customer()
            training_data = training_data.merge(
                get_distance_seller_customer, on="order_id"
            )

        return training_data.dropna()
