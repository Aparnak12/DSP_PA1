###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Discovery Service implementation with Broker support
#
# Created: Spring 2023
#
###############################################

import sys
import time
import argparse
import logging
import zmq
from enum import Enum
from CS6381_MW import discovery_pb2
from CS6381_MW.DiscoveryMW import DiscoveryMW

class DiscoveryAppln:
    class State(Enum):
        INITIALIZE = 0
        CONFIGURE = 1
        RUNNING = 2

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.logger = logger
        self.pub_count = 0
        self.sub_count = 0
        self.reg_pubs = {}
        self.reg_subs = {}
        self.reg_broker = None  # Store broker information
        self.ready = False
        self.mw = None  # Middleware object

    def configure(self, args):
        """Configure the discovery application."""
        try:
            self.logger.info("DiscoveryAppln::configure")
            self.state = self.State.CONFIGURE

            self.pub_count = args.pub_count
            self.sub_count = args.sub_count

            # Initialize middleware
            self.mw = DiscoveryMW(self.logger)
            self.mw.configure(args.addr, args.port)

            self.logger.info("DiscoveryAppln::configure - Configuration complete")
        except Exception as e:
            raise e

    def event_loop(self):
        """Handle incoming requests in an event loop."""
        try:
            self.logger.info("DiscoveryAppln::event_loop - Starting event loop")

            while True:
                # Receive request via middleware
                request = self.mw.recv_request()

                # Process request
                if request.msg_type == discovery_pb2.TYPE_REGISTER:
                    response = self.handle_register(request.register_req)
                elif request.msg_type == discovery_pb2.TYPE_ISREADY:
                    response = self.handle_is_ready()
                elif request.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                    response = self.handle_lookup(request.lookup_req)
                elif request.msg_type == discovery_pb2.TYPE_LOOKUP_BROKER:
                    response = self.handle_broker_lookup()
                else:
                    self.logger.error("Unknown request type")
                    response = discovery_pb2.DiscoveryResp()
                    response.msg_type = discovery_pb2.TYPE_UNKNOWN
                    response.lookup_resp.status = discovery_pb2.STATUS_FAILURE

                # Send response via middleware
                self.mw.send_response(response)
        except Exception as e:
            self.logger.error(f"DiscoveryAppln::event_loop - Exception: {e}")
            raise e

    def handle_register(self, register_req):
        """Handle registration requests."""
        try:
            self.logger.debug("DiscoveryAppln::handle_register")

            role = register_req.role
            name = register_req.info.id
            addr = register_req.info.addr
            port = register_req.info.port
            topics = list(register_req.topiclist)

            if role == discovery_pb2.ROLE_PUBLISHER:
                self.reg_pubs[name] = {"addr": addr, "port": port, "topics": topics}
                self.logger.info(f"Registered publisher: {name} with topics: {topics}")

            elif role == discovery_pb2.ROLE_SUBSCRIBER:
                self.reg_subs[name] = topics
                self.logger.info(f"Registered subscriber: {name} with topics: {topics}")

            elif role == discovery_pb2.ROLE_BROKER:
                self.reg_broker = {"addr": addr, "port": port}  # Store broker details
                self.logger.info(f"Registered broker at {addr}:{port}")

            else:
                response = discovery_pb2.RegisterResp()
                response.status = discovery_pb2.STATUS_FAILURE
                response.reason = "Invalid role"
                return response

            self.check_ready_state()

            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_REGISTER
            response.register_resp.status = discovery_pb2.STATUS_SUCCESS
            response.register_resp.reason = "Registration successful"
            return response
        except Exception as e:
            self.logger.error(f"DiscoveryAppln::handle_register - Exception: {e}")
            response = discovery_pb2.RegisterResp()
            response.status = discovery_pb2.STATUS_FAILURE
            response.reason = str(e)
            return response

    def handle_is_ready(self):
        """Handle is_ready requests."""
        try:
            self.logger.debug("DiscoveryAppln::handle_is_ready")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_ISREADY
            response.isready_resp.status = self.ready
            return response
        except Exception as e:
            self.logger.error(f"DiscoveryAppln::handle_is_ready - Exception: {e}")
            response = discovery_pb2.IsReadyResp()
            response.status = False
            response.error = str(e)
            return response

    def handle_lookup(self, lookup_req):
        """Handle lookup requests."""
        try:
            self.logger.debug(f"DiscoveryAppln::handle_lookup - Topics Requested: {lookup_req.topiclist}")

            # If a broker is registered, return its address
            if self.reg_broker:
                self.logger.info(f"Returning broker address {self.reg_broker['addr']}:{self.reg_broker['port']}")

                response = discovery_pb2.DiscoveryResp()
                response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
                response.lookup_resp.status = discovery_pb2.STATUS_SUCCESS

                broker_info = response.lookup_resp.matched_pubs.add()
                broker_info.id = "broker"
                broker_info.addr = self.reg_broker["addr"]
                broker_info.port = self.reg_broker["port"]

                return response
            else:
                self.logger.error("No broker registered")
                response = discovery_pb2.DiscoveryResp()
                response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
                response.lookup_resp.status = discovery_pb2.STATUS_FAILURE
                return response
        except Exception as e:
            self.logger.error(f"DiscoveryAppln::handle_lookup - Exception: {e}")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            response.lookup_resp.status = discovery_pb2.STATUS_FAILURE
            return response

    def handle_broker_lookup(self):
        """Handle broker lookup requests from publishers."""
        try:
            self.logger.debug("DiscoveryAppln::handle_broker_lookup")

            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_LOOKUP_BROKER

            if self.reg_broker:
                response.lookup_broker_resp.status = discovery_pb2.STATUS_SUCCESS
                broker_info = response.lookup_broker_resp.broker_info
                broker_info.addr = self.reg_broker["addr"]
                broker_info.port = self.reg_broker["port"]
                self.logger.info(f"Broker info provided: {broker_info.addr}:{broker_info.port}")
            else:
                response.lookup_broker_resp.status = discovery_pb2.STATUS_FAILURE
                self.logger.error("No broker registered for lookup")

            return response
        except Exception as e:
            self.logger.error(f"DiscoveryAppln::handle_broker_lookup - Exception: {e}")
            response = discovery_pb2.DiscoveryResp()
            response.msg_type = discovery_pb2.TYPE_LOOKUP_BROKER
            response.lookup_broker_resp.status = discovery_pb2.STATUS_FAILURE
            return response

    def check_ready_state(self):
        """Check if the discovery service is ready."""
        if len(self.reg_pubs) >= self.pub_count and len(self.reg_subs) >= self.sub_count and self.reg_broker:
            self.ready = True
            self.logger.info("System is ready for dissemination")


###################################
# Parse command line arguments
###################################
def parseCmdLineArgs():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Discovery Service")

    parser.add_argument("-a", "--addr", default="localhost", help="IP address to bind to (default: localhost)")
    parser.add_argument("-p", "--port", type=int, default=5555, help="Port to bind to (default: 5555)")
    parser.add_argument("-P", "--pub_count", type=int, default=1, help="Number of publishers in the system (default: 1)")
    parser.add_argument("-S", "--sub_count", type=int, default=1, help="Number of subscribers in the system (default: 1)")

    return parser.parse_args()


###################################
# Main program
###################################
def main():
    try:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        logger = logging.getLogger("DiscoveryAppln")

        args = parseCmdLineArgs()

        discovery_app = DiscoveryAppln(logger)
        discovery_app.configure(args)
        discovery_app.event_loop()

    except Exception as e:
        logging.error(f"Exception in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

