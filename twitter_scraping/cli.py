import argparse
import sys

from . import email
from . import scraper


def twitter_scraping():
    parser = argparse.ArgumentParser(description="Run a twitter scraper")
    parser.add_argument("--follow", action='append', help="User to track (overwrites config)")
    parser.add_argument("--track", action='append', help="Query to track (overwrites config)")
    parser.add_argument("--language", action='append', help="Language to track (overwrites config)")
    parser.add_argument("-c", "--config", type=str, help="JSON configuration")
    parser.add_argument("-o", "--output-dir", type=str, required=True, help="Output directory", dest='output_dir')
    parser.add_argument('-s', '--seconds', type=int, help="Notification frequency (in seconds)")
    parser.add_argument('-m', '--minutes', type=int, help="Notification frequency (in minutes)")
    parser.add_argument('--hours', type=int, help="Notification frequency (in hours)")
    parser.add_argument('-n', '--notify-every', type=int, help="Notification frequency (in tweets)", dest='notify_count')
    parser.add_argument('--no-email', action='store_false', help="Disable emailing", dest='email')
    parser.add_argument('--s3-bucket', type=str, help="S3 Bucket to offload data onto", dest='s3_bucket')
    parser.add_argument('--shard-size', type=int, help="Size of sharded data files", dest='shard_max')

    args = parser.parse_args()

    if len([x for x in [args.seconds, args.minutes, args.hours] if x is not None]) > 1:
        print("Options are mutually exclusive: --seconds, --minutes, and --hours.")
        return 1

    notify_seconds = None
    if args.seconds is not None:
        notify_seconds = args.seconds
    if args.minutes is not None:
        notify_seconds = args.minutes * 60
    if args.hours is not None:
        notify_seconds = args.hours * 3600

    if args.email:
        emailer = email.Emailer()
    else:
        emailer = email.DummyEmailer()
    
    if args.config is None:
        builder = scraper.ScraperBuilder()
    else:
        builder = scraper.ScraperBuilder.load_config(args.config)
    builder = builder.ignore_none()\
              .output_dir(args.output_dir)\
              .notify_count(args.notify_count)\
              .notify_seconds(notify_seconds)\
              .follow(args.follow)\
              .track(args.track)\
              .languages(args.language)\
              .emailer(emailer)\
              .s3_bucket(args.s3_bucket)\
              .shard_max(args.shard_max)
    try:
        with builder.build() as s:
            # Context manages things like files
            pass
    except Exception as e:
        if isinstance(e, KeyboardInterrupt):
            sys.exit(1)
        import traceback
        errmsg = "Exception was raised during run:\n{}".format(traceback.format_exc())
        scraper._LOG.error(errmsg)
        emailer.send_text(message="The run ended in failure:\n{}".format(errmsg),
                          subject="[ERROR] {default_subject}")
        sys.exit(1)
