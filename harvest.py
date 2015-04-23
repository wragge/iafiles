from rstools.client import RSSearchClient
from pymongo import MongoClient, GEO2D
import time
import csv

from credentials import MONGOLAB_URL

SERIES_LIST = [
    {'series': 'SP11/26', 'range': []},
    {'series': 'SP42/1', 'range': []},
    {'series': 'SP115/10', 'range': []},
    {'series': 'ST84/1', 'range': []},
    {'series': 'SP115/1', 'range': []},
    {'series': 'SP11/6', 'range': []},
    {'series': 'SP726/1', 'range': []},
    #{'series': 'B13', 'range': [1901, 2000]},
    {'series': 'B6003', 'range': []},
    {'series': 'J2481', 'range': []},
    {'series': 'J2482', 'range': []},
    {'series': 'J2483', 'range': []},
    {'series': 'J3115', 'range': []},
    {'series': 'BP343/15', 'range': []},
    {'series': 'PP4/2', 'range': []},
    {'series': 'PP6/1', 'range': []},
    {'series': 'K1145', 'range': []},
    {'series': 'E752', 'range': []},
    {'series': 'D2860', 'range': []},
    {'series': 'D5036', 'range': []},
    {'series': 'D596', 'range': []},
    {'series': 'P526', 'range': []},
    {'series': 'P437', 'range': []}
    ]


class SeriesHarvester():
    def __init__(self, series, control=None):
        self.series = series
        self.control = control
        self.total_pages = None
        self.pages_complete = 0
        self.client = RSSearchClient()
        self.prepare_harvest()
        self.items = self.get_db()

    def get_db(self):
        dbclient = MongoClient(MONGOLAB_URL)
        db = dbclient.get_default_database()
        items = db.items
        #items.remove()
        return items

    def get_total(self):
        return self.client.total_results

    def get_db_total(self):
        return self.items.find({'series': self.series}).count()

    def prepare_harvest(self):
        if self.control:
            self.client.search(series=self.series, control=self.control)
        else:
            self.client.search(series=self.series)
        total_results = self.client.total_results
        print '{} items'.format(total_results)
        self.total_pages = (int(total_results) / self.client.results_per_page) + 1
        print self.total_pages

    def start_harvest(self, page=None):
        if not page:
            page = self.pages_complete + 1
        while self.pages_complete < self.total_pages:
            if self.control:
                response = self.client.search(series=self.series, page=page, control=self.control)
            else:
                response = self.client.search(series=self.series, page=page)
            self.items.insert_many(response['results'])
            self.pages_complete += 1
            page += 1
            print '{} pages complete'.format(self.pages_complete)
            time.sleep(1)


def harvest_all_series():
    for series in SERIES_LIST:
        print 'Series {}'.format(series['series'])
        if series['range']:
            for symbol in range(series['range'][0], series['range'][1]):
                print 'Control symbol {}'.format(symbol)
                harvester = SeriesHarvester(series=series['series'], control='*{}/*'.format(symbol))
                harvester.start_harvest()
        else:
            harvester = SeriesHarvester(series=series['series'])
            harvester.start_harvest()


def get_db_items():
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    items = db.items
    #items.remove()
    return items


def delete_one_series(series):
    items = get_db_items()
    deleted = items.delete_many({'series': series})
    print '{} items deleted'.format(deleted.deleted_count)


def change_to_int():
    items = get_db_items()
    for record in items.find({'digitised_pages': {'$ne': 0}}).batch_size(30):
        record['digitised_pages'] = int(record['digitised_pages'])
        items.save(record)


def series_summary():
    items = get_db_items()
    with open('data/series_summary.csv', 'wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['series', 'total described', 'total digitised', 'total pages digitised'])
        for series in SERIES_LIST:
            total = items.count({'series': series['series']})
            total_digitised = items.count({'series': series['series'], 'digitised_status': True})
            pipe = [{"$match": {"series": series['series']}}, {"$group": {"_id": "$series", "total": {"$sum": "$digitised_pages"}}}]
            total_pages = items.aggregate(pipeline=pipe).next()['total']
            print series['series']
            print 'Total: {}'.format(total)
            print 'Total digitised: {} ({:.2f}%)'.format(total_digitised, (total_digitised / float(total) * 100))
            print 'Total digitised pages: {}'.format(total_pages)
            csv_writer.writerow([series['series'], total, total_digitised, '{:.2f}%'.format(total_digitised / float(total) * 100), total_pages])
