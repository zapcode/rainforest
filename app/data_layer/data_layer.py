import pandas as pd

from app.api_client.raninforest_client import RainforestAPIClient

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLayer:
    """
    This is the Logic pipeline which has the logic mentioned
    """

    def __init__(self, api_client: RainforestAPIClient):
        self.api_client = api_client
        self.dataframe = None

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

    def get_review_dataframe(self, product_ids) -> None:
        """
        This has the core logic which takes product ids, fetches the reviews, creates the dataframe and print
        the expected result.
        :param product_ids:
        :return:
        """
        try:
            logger.info(f'Getting data from reviews...')
            final_dataframe = pd.DataFrame()
            for product_id in product_ids:
                reviews = self.api_client.get_reviews(product_id)
                product_dataframe = self.convert_response_to_dataframe(reviews=reviews, product_id=product_id)
                final_dataframe = pd.concat([final_dataframe, product_dataframe])
            self.dataframe = final_dataframe
        except Exception as e:
            logger.error(f'Error found: {e}')