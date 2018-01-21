load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("@bazel_gazelle//:def.bzl", "gazelle")

gazelle(
    name = "gazelle",
    external = "vendored",
    prefix = "github.com/thomaso-mirodin/log2firehose",
)

go_library(
    name = "go_default_library",
    srcs = [
        "firehose_batcher.go",
        "main.go",
    ],
    importpath = "github.com/thomaso-mirodin/log2firehose",
    visibility = ["//visibility:private"],
    deps = [
        "//vendor/github.com/aws/aws-sdk-go/aws/session:go_default_library",
        "//vendor/github.com/aws/aws-sdk-go/service/firehose:go_default_library",
        "//vendor/github.com/jessevdk/go-flags:go_default_library",
        "//vendor/github.com/pkg/errors:go_default_library",
        "//vendor/github.com/thomaso-mirodin/tailer:go_default_library",
    ],
)

go_binary(
    name = "log2firehose",
    embed = [":go_default_library"],
    importpath = "github.com/thomaso-mirodin/log2firehose",
    visibility = ["//visibility:public"],
)
