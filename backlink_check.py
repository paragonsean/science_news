from queue import Queue
import csv
import sys
import re
import argparse
import urllib.error
import urllib.parse
import urllib.request
import http.client
from time import sleep
import tldextract as tld
from threading import Thread
from bs4 import BeautifulSoup as bs, SoupStrainer as ss

parser = argparse.ArgumentParser(description='Check list of URLs for existence of link in html')
parser.add_argument('-d', '--domain', help='The domain you would like to search for a link to', required=True)
parser.add_argument('-i', '--input', help='Text file with list of URLs to check', required=True)
parser.add_argument('-o', '--output', help='Named of csv to output results to', required=True)
parser.add_argument('-v', '--verbose', help='Display URLs and statuses in the terminal', required=False,
                    action='store_true')
parser.add_argument('-w', '--workers', help='Number of workers to create', nargs='?', default='10', required=False)
parser.add_argument('-c', '--createdisavow', help='Create disavow.txt file for Google disavow links tool',
                    required=False, action='store_true')

ARGS = vars(parser.parse_args())
INFILE = ARGS['input']
OUTFILE = ARGS['output']
DOMAIN = ARGS['domain']
NUMBER_OF_WORKERS = int(ARGS['workers'])
VERBOSE = ARGS['verbose']
CREATE_DISAVOW = ARGS['createdisavow']


class backlink(object):
    def __init__(self, url, index, domain):
        self.url = url
        self.status = 'UNKNOWN'
        self.index = index
        self.domain = domain


class worker(Thread):
    def __init__(self, input_queue, output_queue, domain):
        super(worker, self).__init__()
        self.daemon = True
        self.cancelled = False
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.domain = domain

    def url_sanitize(self, url):
        parsed = urllib.parse.urlparse(url)
        return urllib.parse.urlunparse(urllib.parse.quote(x) for x in parsed)

    def check_url(self, url):
        req = urllib.request.Request(url)
        req.add_header('User-Agent',
                       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.57 Safari/537.36')
        try:
            html = urllib.request.urlopen(req).read()
        except urllib.error.HTTPError as e:
            html = e.read()
        soup = bs(html, parse_only=ss('a'))
        link = soup.find_all('a', attrs={'href': re.compile(self.domain)})
        if len(link) > 0:  # link from domain was found
            for i in link:
                if i.has_attr('rel'):
                    return 'NOFOLLOWED'
                else:
                    return 'EXISTS'
        else:
            return 'REMOVED'

    # Workers
    def run(self):
        while not self.cancelled:
            link = self.input_queue.get()
            if link is None:
                self.input_queue.task_done()
                self.input_queue.put(None)
                break
            try:
                link.status = self.check_url(link.url)
            except urllib.error.HTTPError as e:
                link.status = str(e)
            except urllib.error.URLError as e:
                link.status = str(e)
            except http.client.BadStatusLine as e:
                link.status = str(e)
            except UnicodeDecodeError:
                self.check_url(self.sanitize_url(link.url))
            except ConnectionResetError:
                input_queue.put(link)
                break
            self.output_queue.put(link)
            self.input_queue.task_done()
            sleep(0.01)

    def cancel(self):
        self.cancelled = True


class linkcheck(object):

    def __init__(self, domain, in_file, out_file, num_workers, verbose, create_dis):
        self.in_file = in_file
        self.out_file = out_file
        self.num_workers = num_workers
        self.input_queue = Queue()
        self.output_queue = Queue()
        self.domain = domain
        self.links = []
        self.disavow_links = []
        self.number_of_urls = 0
        self.purple = '\033[95m'
        self.orange = '\033[91m'
        self.bold = '\033[1m'
        self.endc = '\033[0m'
        self.verbose = verbose
        self.create_dis = create_dis

    def create_threads(self):
        for i in range(self.num_workers):
            t = worker(self.input_queue, self.output_queue, self.domain)
            t.start()

    def populate_input_queue(self):
        with open(self.in_file, 'r') as f:
            for line in f:
                if line.strip() != '':
                    temp_bl = backlink(line.strip(), self.number_of_urls, self.domain)
                self.input_queue.put(temp_bl)
                self.number_of_urls += 1
        self.input_queue.put(None)

    def write_csv(self):
        with open(self.out_file, 'a') as f:
            c = csv.writer(f, delimiter=',', quotechar='"')
            for i in range(self.number_of_urls):
                link = self.output_queue.get()
                if self.verbose:
                    if link.status == 'EXISTS':
                        print('{}: {}'.format(link.url, self.purple + link.status + self.endc))
                        if self.create_dis:
                            if link.status != 'NOFOLLOWED':
                                dom = tld.extract(link.url)
                                self.disavow_links.append('domain:' + dom.domain + '.' + dom.suffix + '\n')
                    else:
                        print('{}: {}'.format(link.url, self.orange + link.status + self.endc))
                self.links.append(link)
                self.output_queue.task_done()
            self.links.sort(key=lambda x: x.index)
            for i in self.links:
                c.writerow((i.index, i.url, i.status))

    def create_disavow(self):
        # remove duplicate domains
        self.disavow_links = set(self.disavow_links)
        # write disavow file
        if self.create_dis:
            with open('disavow.txt', 'w') as f:
                for i in self.disavow_links:
                    f.write(i)

    def queue_join(self):
        self.input_queue.get()
        self.input_queue.task_done()
        self.input_queue.join()
        self.output_queue.join()

    def run(self):
        lc.create_threads()
        lc.populate_input_queue()
        lc.write_csv()
        lc.create_disavow()
        lc.queue_join()


if __name__ == '__main__':
    lc = linkcheck(DOMAIN, INFILE, OUTFILE, NUMBER_OF_WORKERS, VERBOSE, CREATE_DISAVOW)
    lc.run()