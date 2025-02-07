###############################################
#
# Author: Updated for Broker Fix
# Vanderbilt University
#
# Purpose: Publisher Application with Broker Support
#
# Created: Spring 2023
#
###############################################

import time
import argparse
import logging
from topic_selector import TopicSelector
from CS6381_MW.PublisherMW import PublisherMW
from CS6381_MW import discovery_pb2


class PublisherAppln:

    def __init__(self, logger):
        self.logger = logger
        self.name = None
        self.topiclist = None
        self.iters = None
        self.frequency = None
        self.num_topics = None
        self.mw_obj = None

    def configure(self, args):
        self.logger.info("PublisherAppln::configure")

        self.name = args.name
        self.iters = args.iters
        self.frequency = args.frequency
        self.num_topics = args.num_topics

        ts = TopicSelector()
        self.topiclist = ts.interest(self.num_topics)

        self.mw_obj = PublisherMW(self.logger)
        self.mw_obj.configure(args)

        self.logger.info("PublisherAppln::configure - Configuration complete")

    def driver(self):
        self.logger.info("PublisherAppln::driver")

        self.mw_obj.set_upcall_handle(self)
        self.mw_obj.register(self.name, self.topiclist)
        self.mw_obj.event_loop()

    def invoke_operation(self):
        self.logger.info("PublisherAppln::invoke_operation")

        # Periodically publish messages
        for _ in range(self.iters):
            for topic in self.topiclist:
                data = f"{topic} data at {time.time()}"
                self.mw_obj.disseminate(self.name, topic, data)
            time.sleep(1 / self.frequency)

    def register_response(self, reg_resp):
        self.logger.info("PublisherAppln::register_response")
        if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
            self.logger.info("Registration successful")
            self.invoke_operation()
        else:
            raise ValueError("Publisher registration failed")

    def isready_response(self, isready_resp):
        self.logger.info("PublisherAppln::isready_response")
        if isready_resp.status:
            self.logger.info("Discovery is ready. Starting dissemination.")
        else:
            time.sleep(5)  # Wait before retrying

    def lookup_broker(self, response):  
        self.logger.info("PublisherAppln::lookup_broker")
        # Handle the broker lookup response here
        # For now, just print the response
        self.logger.info(f"Broker lookup response: {response}")


def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Publisher Application")
    parser.add_argument("-n", "--name", default="pub", help="Publisher name")
    parser.add_argument("-a", "--addr", default="localhost", help="Publisher address")
    parser.add_argument("-p", "--port", type=int, default=5577, help="Publisher port")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service address")
    parser.add_argument("-T", "--num_topics", type=int, default=1, help="Number of topics")
    parser.add_argument("-f", "--frequency", type=int, default=1, help="Publishing frequency (per second)")
    parser.add_argument("-i", "--iters", type=int, default=10, help="Number of publishing iterations")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, help="Logging level")
    parser.add_argument('--dissemination', choices=['Direct', 'Broker'], default='Broker', help='Dissemination mode')

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("PublisherAppln")

    args = parseCmdLineArgs()
    pub_app = PublisherAppln(logger)
    pub_app.configure(args)
    pub_app.driver()


if __name__ == "__main__":
    main()

