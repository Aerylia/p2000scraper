import requests
from bs4 import BeautifulSoup
import csv
import re
from datetime import datetime
import pandas as pd
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class P2000Scraper:
    parameters = {
        "headers" : {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'},
        "filename" : "p2000_child_involvement.csv",
        "queries" : ["kind"],
        "labels" : ['AMBU'],
        "params" : {
                "query": "",
                "sop":"1",
                "zoek":"zoeken",
                "cap":"",
                "treg":"",
                "dag_start":"1",
                "mnd_start":"1",
                "yr_start":"2013",
                "dag_stop":"11",
                "mnd_stop":"12",
                "yr_stop":"2016",
                "page":"1"
        },
        "verbose" : True
    }

    def __init__(self, verbose=False):
        self.verbose = verbose
        self.column_names = ['date', 'capcode', 'source','message']

    def visit_page(self, url, headers):
        global failed
        page = requests.get(url, headers=headers, verify=False)

        if not page.ok:
            print "ERROR : Could not read page: ", (url, page.status_code)
            failed.add((url, page.status_code))
            raise Exception("Status code was not ok: " + str(page.status_code))

        soup = BeautifulSoup(page.text, "lxml")
        results = []
        tables = soup('table')
        for row in tables[2]('tr'):
            p2000 = row("td")
            date = datetime.strptime(str(p2000[0].contents[0]), '%d/%m/%Y %H:%M')
            capcode = int(p2000[1]("span")[0].contents[0])
            source = "".join(p2000[3].contents)
            message = re.sub('<.*?>', '', str(p2000[4].contents[0]))

            data = []
            data.append(date)
            data.append(capcode)
            data.append(source)
            data.append(message)
            results.append(data)
        return results

    def iterate_queries(self, headers, queries, params) :
        failed = set()
        data = []
        for query in queries:
            if self.verbose:
                print query
            params['query'] = query
            HOME = "https://www.waarisdebrand.nl/livecap/?"+"&".join(["{}={}".format(x[0],x[1]) for x in params.items()])
            results = self.visit_page(HOME, headers)
            data.extend(results)
            while results:
                params["page"] = str(int(params["page"])+1)
                if self.verbose:
                    print params["page"]
                next_page = "https://www.waarisdebrand.nl/livecap/?"+"&".join(["{}={}".format(x[0],x[1]) for x in params.items()])
                try:
                    results = self.visit_page(next_page, headers)
                    if self.verbose:
                        print "SUCCESS:", next_page
                except:
                    failed.add(next_page)
                    if self.verbose:
                        print "FAILED:", next_page
                data.extend(results)
        return pd.DataFrame(data, columns=self.column_names)

    def store_results(self, filename, data):
        with open(filename, "wb") as f:
            writer = csv.writer(f)
            writer.writerows(data)

    def read_parameters(self, filename):
        """Does not read parameters from file yet! """
        self.verbose = self.parameters['verbose']
        return self.parameters

    def __apply_func(self, to_apply, labels):
        for label in labels:
            if label in to_apply['message']:
                return label
        return 'UNDEFINED'

    def __select_func(self, to_select, data):
        return data['label'][to_select] != 'UNDEFINED'


    def clean_results(self, data, labels):
        data = data.fillna("")
        data['date'] = pd.to_datetime(data['date']).dt.strftime("%Y%m%d")
        data['capcode'] = pd.to_numeric(data['capcode'])
        data['label'] = data.apply(lambda  x: self.__apply_func(x, labels), axis=1)
        data = data.select(lambda x: self.__select_func(x, data), axis=0)
        """data_groupby = data.groupby(['label'])
        aggregates = []
        header = []
        for group in data_groupby.groups.iteritems():
            header.append(group[0])
            agg = data_groupby.get_group(group[0]).groupby('date').size()
            aggregates.append(agg)
        if len(aggregates) > 0:
            aggregate = pd.concat(aggregates, axis=1)
            aggregate.columns = header
            aggregate = aggregate.fillna('')
            return aggregate
        else:
            return pd.DataFrame(columns=self.column_names)
            """
        return data

    def main(self):
        parameters = self.read_parameters("p2000_parameters.json")
        data = self.iterate_queries(parameters['headers'], parameters["queries"], parameters['params'])
        if self.verbose:
            print 'Initial results: '
            print data
        data = self.clean_results(data, parameters['labels'])
        if self.verbose:
            print 'Cleaned results: '
            print data
        self.store_results(parameters['filename'], data)


if __name__ == '__main__':
    scraper = P2000Scraper()
    scraper.main()

