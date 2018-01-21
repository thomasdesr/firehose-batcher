load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("@bazel_gazelle//:def.bzl", "gazelle")

gazelle(
    name = "gazelle",
    external = "vendored",
    prefix = "github.com/thomaso-mirodin/log2firehose",
)

go_library(
    name = "go_default_library",
    srcs = ["firehose_batcher.go"],
    importpath = "github.com/thomaso-mirodin/log2firehose",
    visibility = ["//visibility:private"],
    deps = [
        "//vendor/github.com/aws/aws-sdk-go/service/firehose:go_default_library",
        "//vendor/github.com/pkg/errors:go_default_library",
    ],
)
