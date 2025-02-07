###############################################
#
# Author: Updated for Broker Fix
# Vanderbilt University
#
# Purpose: Subscriber Application with Broker Support
#
# Created: Spring 2023
#
###############################################

import time
import argparse
import logging
from topic_selector import TopicSelector
from CS6381_MW.SubscriberMW import SubscriberMW
from CS6381_MW import discovery_pb2


class SubscriberAppln:

    def __init__(self, logger):
        self.logger = logger
        self.name = None
        self.topiclist = None
        self.num_topics = None
        self.mw_obj = None

    def configure(self, args):
        self.logger.info("SubscriberAppln::configure")

        self.name = args.name
        self.num_topics = args.num_topics

        ts = TopicSelector()
        self.topiclist = ts.interest(self.num_topics)

        self.mw_obj = SubscriberMW(self.logger)
        self.mw_obj.configure(args)

        self.logger.info("SubscriberAppln::configure - Configuration complete")

    def driver(self):
        self.logger.info("SubscriberAppln::driver")

        self.mw_obj.set_upcall_handle(self)
        self.mw_obj.register(self.name, self.topiclist)
        self.mw_obj.event_loop()

    def invoke_operation(self):
        self.logger.info("SubscriberAppln::invoke_operation")
        self.mw_obj.lookup_broker(self.topiclist)

    def register_response(self, reg_resp):
        self.logger.info("SubscriberAppln::register_response")
        if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
            self.logger.info("Registration successful")
        else:
            raise ValueError("Subscriber registration failed")

    def isready_response(self, isready_resp):
        self.logger.info("SubscriberAppln::isready_response")
        if isready_resp.status:
            self.logger.info("Discovery is ready. Waiting for publications.")
        else:
            time.sleep(5)  # Retry after some time

    def lookup_broker(self, response):  
        self.logger.info("SubscriberAppln::lookup_broker")
        if response.status == discovery_pb2.STATUS_SUCCESS:
            self.logger.info("Broker lookup successful")
            for pub in response.matched_pubs:
                self.logger.info(f"Connected to Broker at {pub.addr}:{pub.port}")
        else:
            self.logger.warning("Broker lookup failed")


def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Subscriber Application")
    
    parser.add_argument("-n", "--name", default="sub", help="Subscriber name")
    parser.add_argument("-a", "--addr", default="localhost", help="Subscriber IP address")
    parser.add_argument("-p", "--port", type=int, default=5520, help="Port number for the Subscriber")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service IP:Port")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=2, help="Number of topics to subscribe")
    parser.add_argument("-c", "--config", default="config.ini", help="Configuration file")
    parser.add_argument("-f", "--frequency", type=int, default=1, help="Data reception frequency")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="Number of iterations")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[
        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL
    ], help="Logging level")
    parser.add_argument("--dissemination", choices=["Direct", "Broker"], default="Direct",
                        help="Dissemination strategy: Direct (default) or Broker")

    return parser.parse_args()



def main():
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger("SubscriberAppln")

    args = parseCmdLineArgs()
    sub_app = SubscriberAppln(logger)
    sub_app.configure(args)
    sub_app.driver()


if __name__ == "__main__":
    main()

