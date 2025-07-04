# Changelog

## [1.3.1] 2025-06-23
- Bump backoff to `1.10.0`, bump requests to `2.32.4`, bump
  singer-python to 5.13.2
  [#45](https://github.com/singer-io/tap-eloqua/pull/45) [#47](https://github.com/singer-io/tap-eloqua/pull/47)

## [1.3.0] 2021-03-19
- Adds email groups and contact object [#38](https://github.com/singer-io/tap-eloqua/pull/38)

## [1.2.2] 2020-11-04
- Save bullk bookmark immediately after `sync_id` is retrieved [#34](https://github.com/singer-io/tap-eloqua/pull/34)

## [1.2.1] 2020-10-07
### Added
- Increase bulk sync job timeout to 6 hours [#33](https://github.com/singer-io/tap-eloqua/pull/33)

## [1.2.0] 2020-09-30
### Added
- Increase bulk sync job timeout to 3 hours [#32](https://github.com/singer-io/tap-eloqua/pull/32)

## [1.1.0] 2020-06-12
### Added
- `autoDeleteDuration` parameter to the `POST /api/bulk/2.0/<object>/exports` endpoint to
  autodelete reports after 3 days

## [1.0.0] 2020-01-31
- No change: Releasing from Beta --> GA


[1.2.2]: https://github.com/singer-io/tap-eloqua/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/singer-io/tap-eloqua/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/singer-io/tap-eloqua/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/singer-io/tap-eloqua/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/singer-io/tap-eloqua/compare/v0.6.6...v1.0.0
