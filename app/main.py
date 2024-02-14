import json
import logging
import traceback

from app.pipeline.logic_layer import ReviewPipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def runner(json_path, creds):
    """
    This is the entry runner
    :param json_path: json path of input file with asin
    :param creds: dictionary containing api_key & limit
    :return:
    """
    try:
        logger.info('Runner has started!')
        data = load_json(file_path=json_path)
        product_ids = data.get("ASIN")
        pipeline = ReviewPipeline(creds=creds)
        pipeline.fetch_reviews_and_process(product_ids)
    except Exception as e:
        logger.error(f'Error Found: {e}')
        print(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    # this is where execution starts
    creds = {"api_key": "demo", "limit": 30}
    json_path = "input.json"
    runner(json_path=json_path, creds=creds)
