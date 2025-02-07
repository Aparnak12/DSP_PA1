###############################################
#
# Author: Your Name
# Vanderbilt University
#
# Purpose: Broker Application
#
# Created: Spring 2023
#
###############################################

import time
import argparse
import logging
from enum import Enum
from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2


class BrokerAppln:

    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        MATCH_PUBS = 3,
        RELAY_MESSAGES = 4,
        COMPLETED = 5

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.logger = logger
        self.mw_obj = None

    def configure(self, args):
        try:
            self.logger.info("BrokerAppln::configure")

            self.state = self.State.CONFIGURE
            self.mw_obj = BrokerMW(self.logger)
            self.mw_obj.configure(args)

            self.logger.info("BrokerAppln::configure - Configuration complete")
        except Exception as e:
            raise e

    def driver(self):
        try:
            self.logger.info("BrokerAppln::driver")
            self.mw_obj.set_upcall_handle(self)

            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)

            self.logger.info("BrokerAppln::driver completed")
        except Exception as e:
            raise e

    def invoke_operation(self):
        try:
            self.logger.info("BrokerAppln::invoke_operation")

            if self.state == self.State.REGISTER:
                self.mw_obj.register()
                return None

            elif self.state == self.State.MATCH_PUBS:
                self.mw_obj.lookup_publishers()
                return None

            elif self.state == self.State.RELAY_MESSAGES:
                while True:
                    self.mw_obj.forward_message()
                return None

            elif self.state == self.State.COMPLETED:
                self.mw_obj.disable_event_loop()
                return None

            else:
                raise ValueError("Undefined state")
        except Exception as e:
            raise e

    def register_response(self, reg_resp):
        try:
            self.logger.info("BrokerAppln::register_response")
            if reg_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.state = self.State.MATCH_PUBS
                return 0
            else:
                raise ValueError("Registration failed")
        except Exception as e:
            raise e

    def receive_publisher_list(self, lookup_resp):
        try:
            self.logger.info("BrokerAppln::receive_publisher_list")
            self.mw_obj.connect_to_publishers(lookup_resp)
            self.state = self.State.RELAY_MESSAGES
            return 0
        except Exception as e:
            raise e


def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Broker Application")
    parser.add_argument("-a", "--addr", default="localhost", help="IP addr for broker")
    parser.add_argument("-p", "--port", type=int, default=5578, help="Broker's PUB port")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="Discovery service address")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[
        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL],
        help="Logging level")
    return parser.parse_args()


def main():
    try:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        logger = logging.getLogger("BrokerAppln")

        args = parseCmdLineArgs()
        logger.setLevel(args.loglevel)

        broker_app = BrokerAppln(logger)
        broker_app.configure(args)
        broker_app.driver()
    except Exception as e:
        logger.error(f"Exception in main: {e}")


if __name__ == "__main__":
    main()

