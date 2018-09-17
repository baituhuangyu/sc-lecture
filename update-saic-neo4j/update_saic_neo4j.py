#!/usr/bin/env python
# encoding: utf-8


import sys
import time
from task.tasks import task_by_q
import argparse

reload(sys)
sys.setdefaultencoding('utf8')
from scpy.logger import get_logger

logger = get_logger(__file__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-m', help='model: dx, quantum.',
                        default='dx', choices=["dx", "quantum"])
    args = parser.parse_args()

    while True:
        try:
            task_by_q(model=args.m)
        except KeyboardInterrupt:
            exit(0)
        except Exception as e:
            import traceback
            err = traceback.format_exc()
            logger.exception('task_by_q exception:' + err)
            # exit(0)
            continue
        time.sleep(0.5)
