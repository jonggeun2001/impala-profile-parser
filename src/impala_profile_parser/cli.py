"""Command line entry point."""

import argparse
import logging

from . import __version__


def _positive(value):
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("expected a positive integer") from None
    if number <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description="Convert encoded impalad profile logs to result.parquet")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--input", required=True, help="local encoded log file or directory")
    parser.add_argument("--output", required=True, help="output directory")
    parser.add_argument("--timezone", default="UTC", help="timezone of profile Start/End Time strings (default: UTC)")
    parser.add_argument("--batch-size", type=_positive, default=1024, help="maximum output rows per batch")
    parser.add_argument("--max-line-bytes", type=_positive, default=64 * 1024 * 1024)
    parser.add_argument("--max-decoded-bytes", type=_positive, default=64 * 1024 * 1024)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    # Keep --help/--version available without loading the native Arrow runtime.
    from .output import OutputLocked
    from .pipeline import ConversionError, InvocationError, convert

    try:
        count = convert(args.input, args.output, timezone=args.timezone,
                        batch_size=args.batch_size, max_line_bytes=args.max_line_bytes,
                        max_decoded_bytes=args.max_decoded_bytes)
    except InvocationError as error:
        logging.error("reason=%s", error)
        return 2
    except OutputLocked:
        logging.error("reason=output_locked")
        return 3
    except ConversionError as error:
        logging.error("reason=%s", error)
        return 1
    except Exception as error:
        # Arrow/Thrift exceptions can contain input values. Only emit the class name.
        logging.error("reason=conversion_failed type=%s", type(error).__name__)
        return 1
    except KeyboardInterrupt:
        logging.error("reason=interrupted")
        return 130
    print("Completed: rows={}".format(count))
    return 0
