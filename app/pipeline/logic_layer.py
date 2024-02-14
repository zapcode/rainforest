import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from app.api_client.raninforest_client import RainforestAPIClient
from app.data_layer.data_layer import DataLayer


class ReviewPipeline:
    """
    This is the Logic pipeline which has the logic mentioned
    """

    def __init__(self, creds: dict):
        self.api_client = RainforestAPIClient(**creds)

    @staticmethod
    def convert_response_to_dataframe(reviews, product_id):
        """
        This is a method converts the json response to dataframe for individual product id.
        Intentionally this method is made static because it doesn't have any dependency of class variables
        and can be utilised from outside with creating class object
        :param reviews:
        :param product_id:
        :return:
        """
        reviews_df = pd.DataFrame()
        if reviews:
            for review in reviews:
                reviews_df = reviews_df.append({
                    'asin': product_id,
                    'review': review['id'],
                    'title': review['title'],
                    'body': review['body'],
                    'link': review['link'],
                    'rating': review['rating'],
                    'date': review['date'],
                }, ignore_index=True)
            reviews_df['rating'] = pd.to_numeric(reviews_df['rating'])
        return reviews_df

    def fetch_reviews_and_process(self, product_ids) -> None:
        """
        This has the core logic which takes product ids, fetches the reviews, creates the dataframe and print
        the expected result.
        :param product_ids:
        :return:
        """
        try:
            logger.info(f'Running Pipeline...')
            data_layer_obj = DataLayer(self.api_client)
            data_layer_obj.get_review_dataframe(product_ids=product_ids)
            if data_layer_obj.dataframe.empty:
                print("NO DATA FOUND, CHECK API KEYS OR IF CORRECT PRODUCT IDS ARE ENTERED!")
            else:
                max_avg_product_id = data_layer_obj.dataframe.groupby('asin')['rating'].mean().idxmax()
                product_avg_reviews = data_layer_obj.dataframe.groupby('asin')['rating'].mean()
                product_longest_review = data_layer_obj.dataframe.groupby('asin')['body'].max()
                logger.info(f'Processing Complete...')
                print("PRODUCT ID WITH MAXIMUM AVG RATING:", max_avg_product_id)
                print("PRODUCT WISE AVG REVIEW:", product_avg_reviews.to_dict())
                print("PRODUCT WISE LONGEST REVIEW:", product_longest_review.to_dict())
        except Exception as e:
            logger.error(f'Error found: {e}')
