from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time
from threading import Lock


# changes only add generate report stuff after frontier is empty
class Worker(Thread):
    domainCrawlTimer = {}
    workerLock = Lock()
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests from scraper.py"
        super().__init__(daemon=True)
        
    

    

    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                scraper.generate_report()
                break
            
            urlDomain = scraper.reduceDomain(scraper.extract_domain(tbd_url))
            # print(Worker.domainCrawlTimer)
            while True:
                # Get the lock
                Worker.workerLock.acquire()
                # If the URL is not in the dictionary that holds the last visited time of the url
                if (urlDomain not in Worker.domainCrawlTimer):
                    # Add the URL to the dict, and add the time we accessed it
                    Worker.domainCrawlTimer[urlDomain] = time.time()
                    # Rekease the lock & break out of while loop & download URL
                    Worker.workerLock.release()
                    break
                else:
                    # TimeDelta is the time that has passed since we last crawled the domain
                    timeDelta = time.time()-Worker.domainCrawlTimer[urlDomain]
                    
                    # If we have not exceeded the time delay, release lock & sleep worker until we hit delay
                    if  timeDelta < self.config.time_delay:
                        Worker.workerLock.release()
                        time.sleep(self.config.time_delay-timeDelta+.1)
                    # If we HAVE hit the threshold, break out of while and download URL
                    else:
                        Worker.domainCrawlTimer[urlDomain] = time.time()
                        Worker.workerLock.release()
                        break
                

            resp = download(tbd_url, self.config, self.logger)


            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            #time.sleep(self.config.time_delay)