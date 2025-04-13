# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

### Added
* Add specs for core api

### Changed
* Use column-specific default serializers (rather than always identity)

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

[Unreleased]: https://github.com/joshuadavey/ottla/compare/0.3.1...HEAD
[0.3.1]: https://github.com/joshuadavey/ottla/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/joshuadavey/ottla/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/joshuadavey/ottla/compare/0.1.1...0.2.0
[0.1.1]: https://github.com/joshuadavey/ottla/compare/0.1.0...0.1.1

