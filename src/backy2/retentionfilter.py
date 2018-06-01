#
# This is loosely based on timegaps/timefilter.py and timegaps/main.py
# which are part of the timegaps module. Please see: https://github.com/jgehrcke/timegaps.
#
# The original copyright and license are:
#
# Copyright 2014 Jan-Philip Gehrcke (http://gehrcke.de)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

import re
import time
from collections import OrderedDict
from collections import defaultdict

from backy2.exception import UsageError, InternalError
from backy2.logging import logger


class RetentionFilter():

    valid_categories = ('latest', 'hours', 'days', 'weeks', 'months', 'years')

    # This method is taken from timegaps/main.py, its original name is parse_rules_from_cmdline.
    @classmethod
    def _parse_rules(cls, rules_spec):
        tokens = rules_spec.split(',')
        rules_dict = {}
        for token in tokens:
            if not token:
                raise ValueError('Empty retention policy element.')
            match = re.search(r'([a-z]+)([0-9]+)', token)
            if match:
                category = match.group(1)
                timecount = int(match.group(2))
                if category not in cls.valid_categories:
                    raise ValueError('Time category {} in retention policy is invalid.'.format(category))
                if category in rules_dict:
                    raise ValueError('Time category {} listed more than once in retention policy.'.format(category))
                if timecount <= 0:
                    raise UsageError('Count of time category {} must be a positive integer.'.format(category))
                rules_dict[category] = timecount
                continue
            raise ValueError('Invalid retention policy element {}.'.format(token))

        rules = OrderedDict()
        for category in cls.valid_categories:
            if category in rules_dict:
                rules[category] = rules_dict[category]

        return rules

    def __init__(self, rules_spec, reference_time=None):
        self.reference_time = time.time() if reference_time is None else reference_time
        self.rules = self._parse_rules(rules_spec)
        logger.debug('Retention filter set up with reference time {} and rules {}'.format(self.reference_time, self.rules))

    def filter(self, versions):
        # Category labels without latest
        categories = [category for category in self.rules.keys() if category != 'latest']

        for category in categories:
            setattr(self, '_{}_dict'.format(category), defaultdict(list))

        # Make our own copy
        versions = list(versions)
        # Sort from youngest to oldest
        versions.sort(key=lambda version: version.date.timestamp(), reverse=True)

        # Remove latest versions from consideration if configured
        if 'latest' in self.rules:
            logger.debug('Keeping {} latest versions.'.format(self.rules['latest']))
            del versions[:self.rules['latest']]

        dismissed_versions = []
        for version in versions:
            td = _Timedelta(version.date.timestamp(), self.reference_time)
            logger.debug('Time and time delta for version {} are {} and {}.'
                         .format(version.uid.readable, version.date, td))

            for category in categories:
                timecount = getattr(td, category)
                if timecount <= self.rules[category]:
                    logger.debug('Found matching category {}, timecount {}.'.format(category, timecount))
                    getattr(self, '_{}_dict'.format(category))[timecount].append(version)
                    break
            else:
                # For loop did not break: The item doesn't fit into any category,
                # it's too old
                dismissed_versions.append(version)
                logger.debug('Dismissing version, it doesn\'t fit into any category.')

        for category in categories:
            category_dict = getattr(self, '_{}_dict'.format(category))
            for timecount in category_dict:
                # Keep the oldest of each category, reject the rest
                dismissed_versions.extend(category_dict[timecount][:-1])

        return dismissed_versions


class _Timedelta:
    """
    Represent how many years, months, weeks, days, hours time `t` (float,
    seconds) is earlier than reference time `ref`. Represent these metrics
    with integer attributes (floor division, numbers are cut, i.e. 1.9 years
    would be 1 year).
    There is no implicit summation, each of the numbers is to be considered
    independently. Time units are considered strictly linear: months are
    30 days, years are 365 days, weeks are 7 days, one day is 24 hours.
    """
    def __init__(self, t, reference_time):
        # Expect two numeric values. Might raise TypeError for other types.
        seconds_earlier = reference_time - t
        if seconds_earlier < 0:
            raise InternalError('Time {} isn\'t earlier than reference time {}.'.format(t, reference_time))
        self.hours = int(seconds_earlier // 3600)      # 60 * 60
        self.days = int(seconds_earlier // 86400)      # 60 * 60 * 24
        self.weeks = int(seconds_earlier // 604800)    # 60 * 60 * 24 * 7
        self.months = int(seconds_earlier // 2592000)  # 60 * 60 * 24 * 30
        self.years = int(seconds_earlier // 31536000)  # 60 * 60 * 24 * 365

    def __repr__(self):
        return '<_TimeDelta(hours={}, days={}, weeks={}, months={}, years={})>'\
                    .format(self.hours, self.days, self.weeks, self.months, self.years)
