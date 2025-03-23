SHELL := bash
.ONESHELL:
.SUFFIXES:
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --no-builtin-rules

ifeq ($(VERSION),)
	VERSION:=$(shell cat VERSION)
endif

CLJ_FILES=$(shell find src -name '*.clj' -o -name '*.cljc' -o -name "*.edn")
JAR_DEPS=$(CLJ_FILES) pom.xml README.md

TARGET_DIR:=target
OUTPUT_JAR=$(TARGET_DIR)/com.joshuadavey/ottla-$(VERSION).jar

.PHONY: clean deploy test

.DEFAULT_GOAL:=jar

jar: $(OUTPUT_JAR)

$(OUTPUT_JAR): $(JAR_DEPS)
	clojure -T:build build-jar

deploy: $(OUTPUT_JAR)
	clojure -T:build deploy

test:
	bin/kaocha

clean:
	@rm -rf $(TARGET_DIR)
