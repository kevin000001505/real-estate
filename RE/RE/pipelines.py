# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
import pymysql
import logging
from itemadapter import ItemAdapter

def transform(data):
    try:
        data['community_id'] = data['community']['id']
    except Exception as e:
        logging.debug(e)
    for key, value in data.items():
        if isinstance(value, dict):
            data[key] = ''.join([i for i in value.values() if isinstance(i, str)])
        elif isinstance(value, list):
            data[key] = ','.join(value)
    return data
            
class MongoPipeline:
    collection_name = "community_items"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "items"),
        )

    def open_spider(self, spider):
        if spider.name == "ExtractFirstPage":
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        if spider.name == "ExtractFirstPage":
            self.client.close()

    def process_item(self, item, spider):
        if spider.name == "ExtractFirstPage":
            try:
                self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
            except Exception as e:
                spider.logger.error(f"Error inserting item: {e}")
            return item


class DatabaseInsertionPipeline:
    def open_spider(self, spider):
        if spider.name == "HousingInfo":
            self.connection = pymysql.connect(
                host='localhost',
                user='root',
                password='@America155088',
                database='Real_Estate'
            )
            self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        if spider.name == "HousingInfo":
            self.cursor.close()
            self.connection.close()

    def process_item(self, item, spider):
        if spider.name == "HousingInfo":
            if item:
                item = transform(item)
                self.insert_real_estate(item)
                return item
            else:
                logging.debug("item is None: %s", item)

    def insert_real_estate(self, item):
        item_value = []
        for key, value in item.items():
            item_value.append(value)
        self.cursor.execute("""
                INSERT INTO PropertyTransactions (
                id, date, trans_date, month, address, layout, layout_v2, room_search,
                build_area, build_area_v, building_area, building_total_price, real_park_area,
                unit_price, src_unit_price, total_price, total_price_v, price_tips, context,
                is_special, has_park, unit_has_park, shift_floor, total_floor, build_purpose_str,
                real_park_total_price, real_park_total_price_v, community, trans_rep_year, tags,
                park_type_str, match_type, tips, park_count, history_trans_count, is_new_tag,
                assoc_type, business_circle_id, community_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
            item_value
        ))
        self.connection.commit()
