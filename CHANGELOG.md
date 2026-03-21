# Change Log

All notable changes to this project will be documented in this file.
This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

## [0.4.0] - 2026-03-21

### Added

* New function `ottla.core/trim-topic!` for log retention and cleanup
* New function `ottla.core/list-subscriptions`
* Add `:index-key?` option for `add-topic!` and `ensure-topic` (with default false)
* Add `:max-records` option to consumer to cap batch size (with default 100)
* Promote `ottla.core/topic-subscriptions` to the public API
* Update the optional specs

### Changed

* Track `:updated-at` field in subscriptions
* Consumer now uses `clojure.tools.logging` instead of `println` for warnings and errors

## [0.3.3] - 2026-03-02

### Added

* New function `ottla.core/ensure-topic` for idempotent topic creation

### Changed

* Update pg2 dependency

## [0.3.2] - 2025-05-17

### Added

* Add specs for core api

### Changed

* Update pg2 dependency
* Use column-specific default serializers (rather than always identity)

### Fixed

* Remove reflection of Thread/sleep (via a function)

## [0.3.1] - 2025-04-08

### Added

* Expose helper method for making deserializer xform
* Add `topic-subscriptions` for getting summaries of subscriptions to
  topics and their offsets.
* Topics can be any legal Clojure identifier, not just snake_case names

### Fixed

* Don't increment subscriptions sequence when ensuring
* Topic removal correctly removes the underlying table again

## [0.3.0] - 2025-04-01

### Added

* Expose `commit-offset!` for manual commit mode
* Add `reset-consumer-offset!` to do "rewind"

### Changed

* Wait (up to 100ms) for consumer to establish listen before returning

### Fixed

* Ensure all built-in serializers are loaded
* Stop warning on reflection of Thread/sleep

## [0.2.0] - 2025-03-30

### Changed

* (BREAKING) Change `:val-type` to `:value-type` in parameters for consistency

## [0.1.1] - 2025-03-24

### Changed

* (BREAKING) Change `ottla.core/append` to `ottla.core/append!` to make naming convention.

### Added

* Add some documentation
* Improve build system

## 0.1.0 - 2025-03-22

Initial public release

[Unreleased]: https://github.com/jgdavey/ottla/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/jgdavey/ottla/compare/v0.3.3...v0.4.0
[0.3.3]: https://github.com/jgdavey/ottla/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/jgdavey/ottla/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/jgdavey/ottla/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/jgdavey/ottla/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/jgdavey/ottla/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/jgdavey/ottla/compare/v0.1.0...v0.1.1
