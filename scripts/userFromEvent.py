#!/usr/bin/env python2.7

import argparse

from sg_wrapper_util import get_user_from_event

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Shotgun's EventLogEntry blame",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('event_id', type=int, help='the id of the event you want to retrieve the username from')

    args = parser.parse_args()

    print get_user_from_event(args.event_id)
