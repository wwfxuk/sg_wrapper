#!/usr/bin/env python2.7

import sys
import argparse

from sg_wrapper_util import get_user_from_event

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Shotgun's EventLogEntry blame",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('event_id', type=int, help='the id of the event you want to retrieve the username from')

    args = parser.parse_args()

    try:
        user = get_user_from_event(args.event_id, onlyUsername=False)
    except RuntimeError as e:
        print e
        sys.exit(1)

    if not user:
        print 'Could not retrieve the user from this event'
        sys.exit(1)

    print 'Event %s user infos:' % args.event_id
    for info in [
                 'firstname',
                 'lastname',
                 'email',
                 'login',
                ]:
        if info in user.fields():
            print '%s: %s' % (info.title(), user.field(info))

    if 'permission_rule_set' in user.fields():
        perm = user.permission_rule_set
        if 'display_name' in perm.fields():
            print 'Permission group: %s' % perm.display_name
