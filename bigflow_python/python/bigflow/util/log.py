#!/usr/bin/env python
#encoding=utf-8
# Copyright (c) 2012 Baidu, Inc. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""
A utility wraps Python built-in loggings
"""

import logging
import logging.handlers
import os
import platform
import sys

unicode_type = unicode
bytes_type = str
basestring_type = str

try:
    import curses
except ImportError:
    curses = None

logger = logging.getLogger("com.baidu.bigflow")


def _safe_unicode(obj, encoding='utf-8'):
    """
    Converts any given object to unicode string.

        >>> safeunicode('hello')
        u'hello'
        >>> safeunicode(2)
        u'2'
        >>> safeunicode('\xe1\x88\xb4')
        u'\u1234'
    """
    t = type(obj)
    if t is unicode:
        return obj
    elif t is str:
        return obj.decode(encoding, 'ignore')
    elif t in [int, float, bool]:
        return unicode(obj)
    elif hasattr(obj, '__unicode__') or isinstance(obj, unicode):
        try:
            return unicode(obj)
        except Exception as e:
            return u""
    else:
        return str(obj).decode(encoding, 'ignore')


def _stderr_supports_color():
    import sys
    color = False
    if curses and hasattr(sys.stderr, 'isatty') and sys.stderr.isatty():
        try:
            curses.setupterm()
            if curses.tigetnum("colors") > 0:
                color = True
        except Exception:
            pass
    return color


class LogFormatter(logging.Formatter):
    """Log formatter used in Tornado.

    Key features of this formatter are:

    * Color support when logging to a terminal that supports it.
    * Timestamps on every log line.
    * Robust against str/bytes encoding problems.

    This formatter is enabled automatically by
    `tornado.options.parse_command_line` (unless ``--logging=none`` is
    used).
    """
    DEFAULT_FORMAT = '%(color)s[%(levelname)1.1s %(asctime)s %(module)s:%(lineno)d]%(end_color)s %(message)s'
    DEFAULT_DATE_FORMAT = '%y%m%d %H:%M:%S'
    DEFAULT_COLORS = {
        logging.DEBUG:      4,  # Blue
        logging.INFO:       2,  # Green
        logging.WARNING:    3,  # Yellow
        logging.ERROR:      1,  # Red
    }

    def __init__(self, color=True, fmt=DEFAULT_FORMAT,
                 datefmt=DEFAULT_DATE_FORMAT, colors=None):
        r"""
        :arg bool color: Enables color support.
        :arg string fmt: Log message format.
          It will be applied to the attributes dict of log records. The
          text between ``%(color)s`` and ``%(end_color)s`` will be colored
          depending on the level if color support is on.
        :arg dict colors: color mappings from logging level to terminal color
          code
        :arg string datefmt: Datetime format.
          Used for formatting ``(asctime)`` placeholder in ``prefix_fmt``.

        .. versionchanged:: 3.2

           Added ``fmt`` and ``datefmt`` arguments.
        """
        logging.Formatter.__init__(self, datefmt=datefmt)
        self._fmt = fmt
        if colors is None:
            colors = LogFormatter.DEFAULT_COLORS

        self._colors = {}
        if color and _stderr_supports_color():
            # The curses module has some str/bytes confusion in
            # python3.  Until version 3.2.3, most methods return
            # bytes, but only accept strings.  In addition, we want to
            # output these strings with the logging module, which
            # works with unicode strings.  The explicit calls to
            # unicode() below are harmless in python2 but will do the
            # right conversion in python 3.
            fg_color = (curses.tigetstr("setaf") or
                        curses.tigetstr("setf") or "")
            if (3, 0) < sys.version_info < (3, 2, 3):
                fg_color = unicode_type(fg_color, "ascii")

            for levelno, code in colors.items():
                self._colors[levelno] = unicode_type(curses.tparm(fg_color, code), "ascii")
            self._normal = unicode_type(curses.tigetstr("sgr0"), "ascii")
        else:
            self._normal = ''

    def format(self, record):
        try:
            message = record.getMessage()

            # assert isinstance(message, basestring_type)  # guaranteed by logging
            # Encoding notes:  The logging module prefers to work with character
            # strings, but only enforces that log messages are instances of
            # basestring.  In python 2, non-ascii bytestrings will make
            # their way through the logging framework until they blow up with
            # an unhelpful decoding error (with this formatter it happens
            # when we attach the prefix, but there are other opportunities for
            # exceptions further along in the framework).
            #
            # If a byte string makes it this far, convert it to unicode to
            # ensure it will make it out to the logs.  Use repr() as a fallback
            # to ensure that all byte strings can be converted successfully,
            # but don't do it by default so we don't add extra quotes to ascii
            # bytestrings.  This is a bit of a hacky place to do this, but
            # it's worth it since the encoding errors that would otherwise
            # result are so useless (and tornado is fond of using utf8-encoded
            # byte strings whereever possible).

            record.message = _safe_unicode(message)
        except Exception as e:
            record.message = "Bad message (%r): %r" % (e, record.__dict__)

        record.asctime = self.formatTime(record, self.datefmt)

        if record.levelno in self._colors:
            record.color = self._colors[record.levelno]
            record.end_color = self._normal
        else:
            record.color = record.end_color = ''

        formatted = self._fmt % record.__dict__

        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            # exc_text contains multiple lines.  We need to _safe_unicode
            # each line separately so that non-utf8 bytes don't cause
            # all the newlines to turn into '\n'.
            lines = [formatted.rstrip()]
            lines.extend(_safe_unicode(ln) for ln in record.exc_text.split('\n'))
            formatted = '\n'.join(lines)
        return formatted.replace("\n", "\n    ")


def enable_pretty_logging(
        logger,
        level,
        log_file="",
        backupCount=10,
        maxBytes=10000000):
    """Turns on formatted logging output as configured.
    """
    if logger is None:
        raise error.BigflowPlanningException("logger cannot be None")

    if "__PYTHON_IN_REMOTE_SIDE" in os.environ:
        # Do not do logging at runtime
        logger.addHandler(logging.NullHandler())
    else:
        logger.setLevel(level)

        if log_file:
            channel = logging.handlers.RotatingFileHandler(
                filename=log_file,
                maxBytes=maxBytes,
                backupCount=backupCount)
            channel.setFormatter(LogFormatter(color=False))
            logger.addHandler(channel)

        if not logger.handlers:
            # Set up color if we are in a tty and curses is installed
            channel = logging.StreamHandler()
            channel.setFormatter(LogFormatter())
            logger.addHandler(channel)


def enable_pretty_logging_at_debug(
        logger,
        level,
        log_file="",
        backupCount=10,
        maxBytes=10000000):
    """Turns on formatted logging output only at DEBUG level
    """
    if level == logging.DEBUG:
        enable_pretty_logging(logger, level, log_file, backupCount, maxBytes)
    else:
        logger.addHandler(logging.NullHandler())


def init_log(level=logging.INFO):
    """ init_log - initialize log module

    Args:
      level (str):     msg above the level will be displayed
                       DEBUG < INFO < WARNING < ERROR < CRITICAL \n
                       ``the default value is logging.INFO``

    Raises:
      OSError: fail to create log directories
      IOError: fail to open log file
    """
    log_file = os.environ.get("BIGFLOW_LOG_FILE", "")
    if log_file:
        log_file = os.path.abspath(log_file + ".log")
        print >> sys.stderr, "Bigflow Log file is written to [%s]" % log_file
    enable_pretty_logging(logger, level, log_file=log_file)
    #enable_pretty_logging_at_debug(
    #        logging.getLogger("pbrpc"),
    #        level,
    #        log_file=log_file)
    #enable_pretty_logging_at_debug(
    #        logging.getLogger("pbrpcrpc_client"),
    #        level,
    #        log_file=log_file)


init_log(logging.INFO)
