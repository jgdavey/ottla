# Change Log
All notable changes to this project will be documented in this file. This change log follows the conventions of [keepachangelog.com](http://keepachangelog.com/).

## [Unreleased]

### Added
* Expose helper method for making deserializer xform

### Fixed

* Don't increment subscriptions sequence when ensuring

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

[Unreleased]: https://github.com/joshuadavey/ottla/compare/0.3.0...HEAD
[0.3.0]: https://github.com/joshuadavey/ottla/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/joshuadavey/ottla/compare/0.1.1...0.2.0
[0.1.1]: https://github.com/joshuadavey/ottla/compare/0.1.0...0.1.1

