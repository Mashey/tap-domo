import singer
from singer.catalog import write_catalog
from tap_domo.discovery import discover
from tap_domo.sync import sync

# Fill in any required config keys from the config.json here
REQUIRED_CONFIG_KEYS = [...]

LOGGER = singer.get_logger()


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    catalog = args.catalog if args.catalog else discover()

    if args.discover:
        write_catalog(catalog)
    else:
        sync(args.config, args.state, catalog)


if __name__ == '__main__':
    main()
